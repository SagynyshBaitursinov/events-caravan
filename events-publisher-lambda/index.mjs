import {PublishBatchCommand, SNSClient} from '@aws-sdk/client-sns';

const {APP_NAME, AWS_ENDPOINT} = process.env;

if (!APP_NAME) {
    throw new Error('APP_NAME environment variable is required to derive topic names');
}

const MAX_ENTRIES_PER_BATCH = 10;
const MAX_BYTES_PER_BATCH = 262144;

const snsClient = new SNSClient({
    endpoint: AWS_ENDPOINT,
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
        byteSize: measureByteSize(message, messageAttributes)
    };
}

/**
 * Entries are prepared up front so that consecutive records of the same entity can be published
 * in one call. A record that cannot be prepared stops the preparation instead of failing the whole
 * batch: everything before it still gets published, and the stream is rewound to it afterwards.
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
                'Failed to prepare stream record %s, retrying from it',
                record.dynamodb.SequenceNumber,
                error);

            return {entries, unpreparedSequenceNumber: record.dynamodb.SequenceNumber};
        }
    }

    return {entries, unpreparedSequenceNumber: null};
}

async function publishBatch(batch) {
    let response;

    try {
        response = await snsClient.send(
            new PublishBatchCommand({
                TopicArn: batch[0].topicArn,
                PublishBatchRequestEntries: batch.map((entry, index) => ({
                    Id: String(index),
                    Message: entry.message,
                    MessageAttributes: entry.messageAttributes
                }))
            }));
    } catch (error) {
        console.error(
            'Failed to publish %d stream records to %s, retrying from record %s',
            batch.length,
            batch[0].topicArn,
            batch[0].streamSequenceNumber,
            error);

        return batch[0].streamSequenceNumber;
    }

    const failures = response.Failed ?? [];
    if (failures.length === 0) return null;

    const firstFailure = failures.reduce(
        (earliest, failure) => Number(failure.Id) < Number(earliest.Id) ? failure : earliest);

    console.error(
        'Failed to publish %d of %d stream records to %s, retrying from record %s (%s: %s)',
        failures.length,
        batch.length,
        batch[0].topicArn,
        batch[Number(firstFailure.Id)].streamSequenceNumber,
        firstFailure.Code,
        firstFailure.Message);

    return batch[Number(firstFailure.Id)].streamSequenceNumber;
}

/**
 * A batch is flushed once it is full, once it would exceed the payload limit, or once the next
 * entry belongs to another topic. Grouping only consecutive entries keeps the records of one
 * entity in stream order within their topic.
 */
async function publishEntries(entries) {
    let batch = [];
    let batchByteSize = 0;

    for (const entry of entries) {
        const isBatchFull = batch.length === MAX_ENTRIES_PER_BATCH;
        const exceedsPayloadLimit = batchByteSize + entry.byteSize > MAX_BYTES_PER_BATCH;
        const belongsToAnotherTopic = batch.length > 0 && batch[0].topicArn !== entry.topicArn;

        if (batch.length > 0 && (isBatchFull || exceedsPayloadLimit || belongsToAnotherTopic)) {
            const failedSequenceNumber = await publishBatch(batch);
            if (failedSequenceNumber) return failedSequenceNumber;

            batch = [];
            batchByteSize = 0;
        }

        batch.push(entry);
        batchByteSize += entry.byteSize;
    }

    return batch.length > 0 ? publishBatch(batch) : null;
}

export const handler = async (event, context) => {
    topicArnPrefix ??= deriveTopicArnPrefix(context.invokedFunctionArn);

    const {entries, unpreparedSequenceNumber} = prepareEntries(event.Records);

    const failedSequenceNumber = await publishEntries(entries) ?? unpreparedSequenceNumber;

    return failedSequenceNumber
        ? {batchItemFailures: [{itemIdentifier: failedSequenceNumber}]}
        : {batchItemFailures: []};
};
