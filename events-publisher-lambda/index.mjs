import {PublishBatchCommand, SNSClient} from '@aws-sdk/client-sns';

const {APP_NAME, AWS_ENDPOINT_OVERRIDE} = process.env;

if (!APP_NAME) {
    throw new Error('APP_NAME environment variable is required to derive topic names');
}

const MAX_ENTRIES_PER_BATCH = 10;
const MAX_BYTES_PER_BATCH = 262144;

const snsClient = new SNSClient({
    ...(AWS_ENDPOINT_OVERRIDE && {endpoint: AWS_ENDPOINT_OVERRIDE}),
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

function deriveEntityReference(shardedEntityReference) {
    const parts = shardedEntityReference.split('#');

    if (parts.length < 3) {
        throw new Error(`Malformed entityReference: '${shardedEntityReference}'`);
    }

    return {
        entityName: parts[0],
        entityId: parts.slice(1, -1).join('#')
    };
}

function constructMessage(newImage, entityReference) {
    const envelope = JSON.stringify({
        entityReference,
        eventName: newImage.eventName.S,
        timestamp: newImage.timestamp.S
    });

    return `${envelope.slice(0, -1)}`
        + `,"sequenceNumber":${newImage.sequenceNumber.N}`
        + `,"payload":${newImage.payload.S}}`;
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

function prepareEntries(records) {
    const entries = [];

    for (const record of records) {
        if (record.eventName !== 'INSERT') continue;
        if (!record.dynamodb?.NewImage) continue;

        try {
            entries.push(prepareEntry(record));
        } catch (error) {
            console.error(
                'Failed to prepare stream record %s, retrying from it',
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

async function publishBatch(entries) {
    const topicArn = entries[0].topicArn;
    let response;

    try {
        response = await snsClient.send(
            new PublishBatchCommand({
                TopicArn: topicArn,
                PublishBatchRequestEntries: entries.map((entry, index) => ({
                    Id: String(index),
                    Message: entry.message,
                    MessageAttributes: entry.messageAttributes
                }))
            }));
    } catch (error) {
        console.error(
            'Failed to publish %d stream records to %s',
            entries.length,
            topicArn,
            error);

        return false;
    }

    for (const successful of response.Successful ?? []) {
        entries[Number(successful.Id)].published = true;
    }

    const failures = response.Failed ?? [];
    if (failures.length === 0) return true;

    console.error(
        'Failed to publish %d of %d stream records to %s (%s: %s)',
        failures.length,
        entries.length,
        topicArn,
        failures[0].Code,
        failures[0].Message);

    return false;
}

/**
 * Publishing stops at the first batch that did not fully succeed, and the stream is rewound to the
 * earliest record that was not published. Batches no longer follow stream order, so the record that
 * failed is not necessarily the earliest one left unpublished: entries of another topic may sit
 * before it and not have been attempted at all.
 */
async function publishEntries(entries) {
    for (const batch of groupIntoBatches(entries)) {
        if (!await publishBatch(batch.entries)) break;
    }

    return entries.find(entry => !entry.published)?.streamSequenceNumber ?? null;
}

export const handler = async (event, context) => {
    topicArnPrefix ??= deriveTopicArnPrefix(context.invokedFunctionArn);

    const {entries, unpreparedSequenceNumber} = prepareEntries(event.Records);

    const failedSequenceNumber = await publishEntries(entries) ?? unpreparedSequenceNumber;

    if (!failedSequenceNumber) return {batchItemFailures: []};

    console.error('Retrying from stream record %s', failedSequenceNumber);

    return {batchItemFailures: [{itemIdentifier: failedSequenceNumber}]};
};
