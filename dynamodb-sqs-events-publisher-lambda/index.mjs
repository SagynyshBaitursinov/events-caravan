import {Agent as HttpsAgent} from 'node:https';
import {BatchRequestTooLongException, PublishBatchCommand, PublishCommand, SNSClient} from '@aws-sdk/client-sns';

const {APP_NAME, MAX_CONCURRENT_PUBLISHES = '10'} = process.env;

if (!APP_NAME) {
    throw new Error('APP_NAME environment variable is required to derive topic names');
}

const MAX_ENTRIES_PER_BATCH = 10;
const maxConcurrentPublishes = Number(MAX_CONCURRENT_PUBLISHES);

if (!Number.isInteger(maxConcurrentPublishes) || maxConcurrentPublishes < 1) {
    throw new Error(`MAX_CONCURRENT_PUBLISHES must be a positive integer, got '${MAX_CONCURRENT_PUBLISHES}'`);
}

const snsClient = new SNSClient({
    requestHandler: {
        connectionTimeout: 1000,
        requestTimeout: 3000,
        httpsAgent: new HttpsAgent({keepAlive: true, maxSockets: maxConcurrentPublishes})
    }
});

function createLimiter(limit) {
    let active = 0;
    const waiters = [];

    return async function withLimit(task) {
        if (active >= limit) {
            await new Promise(resolve => waiters.push(resolve));
        }

        active++;

        try {
            return await task();
        } finally {
            active--;
            waiters.shift()?.();
        }
    };
}

const limitPublish = createLimiter(maxConcurrentPublishes);

let topicArnPrefix;

function deriveTopicArnPrefix(invokedFunctionArn) {
    const [, partition, , region, accountId] = invokedFunctionArn.split(':');
    return `arn:${partition}:sns:${region}:${accountId}:${APP_NAME}_`;
}

function deriveEntityReference(shardedEntityReference) {
    const parts = shardedEntityReference.split('#');

    if (parts.length !== 3) {
        throw new Error(`Malformed entityReference: '${shardedEntityReference}'`);
    }

    return {
        entityName: parts[0],
        entityId: parts[1]
    };
}

function constructMessage(newImage, entityReference) {
    const envelope = JSON.stringify({
        entityReference,
        eventName: newImage.eventName.S,
        timestamp: newImage.timestamp.S
    });

    return envelope.slice(0, -1)
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

function prepareEntry(record) {
    const newImage = record.dynamodb.NewImage;
    const entityReference = deriveEntityReference(newImage.entityReference.S);

    return {
        streamSequenceNumber: record.dynamodb.SequenceNumber,
        topicArn: topicArnPrefix + entityReference.entityName,
        message: constructMessage(newImage, entityReference),
        messageAttributes: constructMessageAttributes(newImage, entityReference),
        published: false
    };
}

function prepareEntries(records) {
    const entries = [];

    for (const record of records) {
        if (record.eventName !== 'INSERT') continue;
        if (!record.dynamodb?.NewImage) return {entries, earliestUnpreparedSequenceNumber: record.dynamodb?.SequenceNumber};

        try {
            entries.push(prepareEntry(record));
        } catch (error) {
            console.error({
                message: 'Failed to prepare stream record, retrying from it',
                streamSequenceNumber: record.dynamodb.SequenceNumber,
                error
            });

            return {entries, earliestUnpreparedSequenceNumber: record.dynamodb.SequenceNumber};
        }
    }

    return {entries, earliestUnpreparedSequenceNumber: null};
}

function groupIntoBatches(entries) {
    const batchesByTopic = new Map();

    for (const entry of entries) {
        const batches = batchesByTopic.get(entry.topicArn) ?? [];
        const openBatch = batches[batches.length - 1];

        if (openBatch && openBatch.length < MAX_ENTRIES_PER_BATCH) {
            openBatch.push(entry);
        } else {
            batches.push([entry]);
        }

        batchesByTopic.set(entry.topicArn, batches);
    }

    return [...batchesByTopic.values()].flat();
}

async function publishEntry(entry) {
    try {
        await limitPublish(() => snsClient.send(
            new PublishCommand({
                TopicArn: entry.topicArn,
                Message: entry.message,
                MessageAttributes: entry.messageAttributes
            })));

        entry.published = true;
    } catch (error) {
        console.error({
            message: 'Failed to publish stream record',
            streamSequenceNumber: entry.streamSequenceNumber,
            topicArn: entry.topicArn,
            error
        });
    }
}

async function publishBatch(entries) {
    const topicArn = entries[0].topicArn;
    let response;

    try {
        response = await limitPublish(() => snsClient.send(
            new PublishBatchCommand({
                TopicArn: topicArn,
                PublishBatchRequestEntries: entries.map((entry, index) => ({
                    Id: String(index),
                    Message: entry.message,
                    MessageAttributes: entry.messageAttributes
                }))
            })));
    } catch (error) {
        if (error instanceof BatchRequestTooLongException) {
            console.warn({
                message: 'Batch rejected as too long, republishing its stream records one by one',
                topicArn,
                recordCount: entries.length
            });

            await Promise.all(entries.map(publishEntry));
            return;
        }

        console.error({
            message: 'Failed to publish batch of stream records',
            topicArn,
            recordCount: entries.length,
            streamSequenceNumbers: entries.map(entry => entry.streamSequenceNumber),
            error
        });

        return;
    }

    for (const successful of response.Successful ?? []) {
        entries[Number(successful.Id)].published = true;
    }

    const failures = response.Failed ?? [];

    if (failures.length > 0) {
        console.error({
            message: 'Failed to publish some stream records of a batch',
            topicArn,
            recordCount: entries.length,
            failureCount: failures.length,
            failures: failures.map(failure => ({
                streamSequenceNumber: entries[Number(failure.Id)].streamSequenceNumber,
                errorCode: failure.Code,
                errorMessage: failure.Message,
                senderFault: failure.SenderFault
            }))
        });
    }
}

async function publishEntriesAndReturnEarliestFailedSequenceNumber(entries) {
    await Promise.all(
        groupIntoBatches(entries)
            .map(batch => publishBatch(batch)
                .catch(error => console.error({
                    message: 'Unexpected error publishing batch of stream records',
                    topicArn: batch[0]?.topicArn,
                    error
                }))));

    return entries.find(entry => !entry.published)?.streamSequenceNumber ?? null;
}

export const handler = async (event, context) => {
    topicArnPrefix ??= deriveTopicArnPrefix(context.invokedFunctionArn);

    const {entries, earliestUnpreparedSequenceNumber} = prepareEntries(event.Records);

    const earliestFailedSequenceNumber = await publishEntriesAndReturnEarliestFailedSequenceNumber(entries) ?? earliestUnpreparedSequenceNumber;

    if (!earliestFailedSequenceNumber) {
        console.debug({
            message: 'Published all stream records of the invocation',
            recordCount: entries.length
        });

        return {batchItemFailures: []};
    }

    console.error({
        message: 'Reporting earliest unpublished stream record as a batch item failure',
        streamSequenceNumber: earliestFailedSequenceNumber
    });

    return {batchItemFailures: [{itemIdentifier: earliestFailedSequenceNumber}]};
};
