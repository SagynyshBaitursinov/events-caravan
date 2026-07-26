import {PublishBatchCommand, PublishCommand, SNSClient} from '@aws-sdk/client-sns';

const {APP_NAME} = process.env;

if (!APP_NAME) {
    throw new Error('APP_NAME environment variable is required to derive topic names');
}

const KEY_SEPARATOR = '#';
const MAX_ENTRIES_PER_BATCH = 10;
const MAX_BYTES_PER_BATCH = 262144;
const MAX_CONCURRENT_PUBLISHES = 10;

// Region, credentials and AWS_ENDPOINT_URL are all picked up from the environment by the SDK.
const snsClient = new SNSClient({
    requestHandler: {
        connectionTimeout: 1000,
        requestTimeout: 3000
    }
});

let topicArnPrefix;

function deriveTopicArnPrefix(invokedFunctionArn) {
    const [, partition, , region, accountId] = invokedFunctionArn.split(':');
    return `arn:${partition}:sns:${region}:${accountId}:${APP_NAME}_`;
}

/**
 * The partition key is 'entityName#entityId#shardIndex'. Neither part of the entity reference may
 * contain the separator, so anything but exactly three parts means the item was not written by the
 * event store and cannot be forwarded.
 */
function deriveEntityReference(partitionKey) {
    const parts = partitionKey.split(KEY_SEPARATOR);

    if (parts.length !== 3 || !parts[0] || !parts[1]) {
        throw new Error(
            `Malformed entityReference: '${partitionKey}', `
            + `expected 'entityName${KEY_SEPARATOR}entityId${KEY_SEPARATOR}shardIndex'`);
    }

    return {entityName: parts[0], entityId: parts[1]};
}

/**
 * The payload reaches us as the JSON the producer already serialized, so it is spliced in verbatim
 * rather than parsed and written back out: re-serializing would only cost CPU and could alter it
 * (number precision, property order). Every other property is a scalar that JSON.stringify escapes,
 * and sequenceNumber is a DynamoDB number, so its string form is already valid JSON.
 */
function constructMessage(newImage, entityReference) {
    return '{"entityReference":' + JSON.stringify(entityReference)
        + ',"eventName":' + JSON.stringify(newImage.eventName.S)
        + ',"sequenceNumber":' + newImage.sequenceNumber.N
        + ',"timestamp":' + JSON.stringify(newImage.timestamp.S)
        + ',"payload":' + newImage.payload.S
        + '}';
}

function constructMessageAttributes(newImage, entityReference) {
    return {
        entityName: {
            DataType: 'String',
            StringValue: entityReference.entityName
        },
        eventName: {
            DataType: 'String',
            StringValue: newImage.eventName.S
        }
    };
}

function measureByteSize(message, messageAttributes) {
    let byteSize = Buffer.byteLength(message);

    for (const [name, attribute] of Object.entries(messageAttributes)) {
        byteSize += Buffer.byteLength(name)
            + Buffer.byteLength(attribute.DataType)
            + Buffer.byteLength(attribute.StringValue);
    }

    return byteSize;
}

function prepareEntry(record) {
    const newImage = record.dynamodb.NewImage;
    const entityReference = deriveEntityReference(newImage.entityReference.S);

    const message = constructMessage(newImage, entityReference);
    const messageAttributes = constructMessageAttributes(newImage, entityReference);

    return {
        streamSequenceNumber: record.dynamodb.SequenceNumber,
        topicArn: topicArnPrefix + entityReference.entityName,
        message,
        messageAttributes,
        byteSize: measureByteSize(message, messageAttributes),
        published: false
    };
}

/**
 * Preparation stops at the first record that cannot be turned into a message. The stream has to be
 * rewound to that record anyway, so preparing the ones behind it would only publish them twice.
 * Note that 'record.eventName' is the stream operation, not the domain event name; the event source
 * mapping is expected to filter on INSERT already, and events are immutable, so the guard below
 * only matters if that filter is missing.
 */
function prepareEntries(records) {
    const entries = [];

    for (const record of records) {
        if (record.eventName !== 'INSERT') continue;
        if (!record.dynamodb?.NewImage) continue;

        try {
            entries.push(prepareEntry(record));
        } catch (error) {
            console.error(
                'Failed to prepare stream record %s, rewinding to it',
                record.dynamodb.SequenceNumber,
                error);

            return {entries, unpreparedSequenceNumber: record.dynamodb.SequenceNumber};
        }
    }

    return {entries, unpreparedSequenceNumber: null};
}

/**
 * All entries of one topic go into as few batches as possible, regardless of how the records were
 * interleaved in the stream, since neither a standard topic nor a standard queue preserves order
 * anyway. A batch holds at most 10 entries, and SNS counts its payload limit per request rather
 * than per message, so entries are also cut off at 256 KB in total.
 */
function groupIntoBatches(entries) {
    const batchesByTopic = new Map();

    for (const entry of entries) {
        const batches = batchesByTopic.get(entry.topicArn) ?? [];
        const openBatch = batches[batches.length - 1];

        const fitsIntoOpenBatch = openBatch
            && openBatch.entries.length < MAX_ENTRIES_PER_BATCH
            && openBatch.byteSize + entry.byteSize <= MAX_BYTES_PER_BATCH;

        if (fitsIntoOpenBatch) {
            openBatch.entries.push(entry);
            openBatch.byteSize += entry.byteSize;
        } else {
            batches.push({entries: [entry], byteSize: entry.byteSize});
        }

        batchesByTopic.set(entry.topicArn, batches);
    }

    return [...batchesByTopic.values()].flat();
}

async function publishSingleEntry(entry) {
    await snsClient.send(
        new PublishCommand({
            TopicArn: entry.topicArn,
            Message: entry.message,
            MessageAttributes: entry.messageAttributes
        }));

    entry.published = true;
}

async function publishBatch(entries) {
    const response = await snsClient.send(
        new PublishBatchCommand({
            TopicArn: entries[0].topicArn,
            PublishBatchRequestEntries: entries.map((entry, index) => ({
                Id: String(index),
                Message: entry.message,
                MessageAttributes: entry.messageAttributes
            }))
        }));

    for (const successful of response.Successful ?? []) {
        entries[Number(successful.Id)].published = true;
    }

    const failures = response.Failed ?? [];
    if (failures.length === 0) return;

    console.error(
        'SNS rejected %d of %d events for %s (%s: %s)',
        failures.length,
        entries.length,
        entries[0].topicArn,
        failures[0].Code,
        failures[0].Message);
}

/**
 * A batch of one is published on its own with Publish instead of PublishBatch. That is the case
 * where an entry did not fit next to any other because it is close to the per-request limit, and
 * publishing it one by one drops the batch envelope that would otherwise push it over. An entry
 * that is genuinely too large fails here, is reported back to the stream and ends up in the DLQ.
 *
 * Failures are logged and swallowed: which entries were published is tracked on the entries
 * themselves, and the handler derives one rewind point from all of them at the end.
 */
async function publishEntries(entries) {
    try {
        if (entries.length === 1) {
            await publishSingleEntry(entries[0]);
        } else {
            await publishBatch(entries);
        }
    } catch (error) {
        console.error(
            'Failed to publish %d events to %s',
            entries.length,
            entries[0].topicArn,
            error);
    }
}

/**
 * Batches are independent - they either target different topics or were split because they did not
 * fit into one request - so they are published concurrently. Workers pull from a shared iterator to
 * keep the number of connections to SNS bounded no matter how many entities an invocation covers.
 */
async function publishBatches(batches) {
    const pendingBatches = batches.values();

    await Promise.all(
        Array.from(
            {length: Math.min(MAX_CONCURRENT_PUBLISHES, batches.length)},
            async () => {
                for (const batch of pendingBatches) {
                    await publishEntries(batch.entries);
                }
            }));
}

/**
 * Batches no longer follow stream order, so the earliest record left unpublished is not necessarily
 * one that failed: entries of another topic may sit before it and have failed for their own reason.
 * Rewinding to the earliest of them republishes whatever succeeded after it, which is why consumers
 * have to be idempotent.
 */
export const handler = async (event, context) => {
    topicArnPrefix ??= deriveTopicArnPrefix(context.invokedFunctionArn);

    const {entries, unpreparedSequenceNumber} = prepareEntries(event.Records);

    await publishBatches(groupIntoBatches(entries));

    const failedSequenceNumber = entries.find(entry => !entry.published)?.streamSequenceNumber
        ?? unpreparedSequenceNumber;

    if (!failedSequenceNumber) return {batchItemFailures: []};

    console.error('Retrying stream from record %s', failedSequenceNumber);

    return {batchItemFailures: [{itemIdentifier: failedSequenceNumber}]};
};
