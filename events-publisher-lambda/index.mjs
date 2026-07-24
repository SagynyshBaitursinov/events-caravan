import {PublishCommand, SNSClient} from '@aws-sdk/client-sns';
import {unmarshall} from '@aws-sdk/util-dynamodb';

const {APP_NAME, AWS_ENDPOINT} = process.env;

if (!APP_NAME) {
    throw new Error('APP_NAME environment variable is required to derive topic names');
}

const snsClient = new SNSClient({endpoint: AWS_ENDPOINT});

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

function constructMessage(unmarshalledImage, entityReference) {
    const envelope = JSON.stringify({
        entityReference,
        sequenceNumber: unmarshalledImage.sequenceNumber,
        eventName: unmarshalledImage.eventName,
        timestamp: unmarshalledImage.timestamp
    });

    return `${envelope.slice(0, -1)},"payload":${unmarshalledImage.payload}}`;
}

function constructMessageAttributes(unmarshalledImage, entityReference) {
    return {
        entityName: {
            DataType: 'String',
            StringValue: entityReference.entityName
        },
        eventName: {
            DataType: 'String',
            StringValue: unmarshalledImage.eventName
        }
    };
}

async function processRecord(record, topicArnPrefix) {
    if (record.eventName !== 'INSERT') return;

    const newImage = record.dynamodb?.NewImage;
    if (!newImage) return;

    const unmarshalledImage = unmarshall(newImage);
    const entityReference = deriveEntityReference(unmarshalledImage.entityReference);

    await snsClient.send(
        new PublishCommand({
            TopicArn: topicArnPrefix + entityReference.entityName,
            Message: constructMessage(unmarshalledImage, entityReference),
            MessageAttributes: constructMessageAttributes(unmarshalledImage, entityReference)
        }));
}

export const handler = async (event, context) => {
    const topicArnPrefix = deriveTopicArnPrefix(context.invokedFunctionArn);

    for (const record of event.Records) {
        try {
            await processRecord(record, topicArnPrefix);
        } catch (error) {
            console.error(
                'Failed to publish stream record %s, retrying from it',
                record.dynamodb.SequenceNumber,
                error);

            return {batchItemFailures: [{itemIdentifier: record.dynamodb.SequenceNumber}]};
        }
    }

    return {batchItemFailures: []};
};
