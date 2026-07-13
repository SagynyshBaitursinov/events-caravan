import {PublishCommand, SNSClient} from '@aws-sdk/client-sns';
import {unmarshall} from '@aws-sdk/util-dynamodb';

const {AWS_ACCOUNT_ID, AWS_REGION, AWS_ENDPOINT} = process.env;

const snsClient = new SNSClient({
    region: AWS_REGION,
    endpoint: AWS_ENDPOINT
});

function deriveTopicName(entityReference) {
    const entityName = entityReference.split('#')[0];
    return `${entityName}`;
}

function deriveTopicArn(topicName) {
    return `arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:${topicName}`;
}

function constructMessageBody(unmarshalledImage) {
    const entityReferenceSplit = unmarshalledImage.entityReference.split('#');
    return {
        entityReference: {
            entityName: entityReferenceSplit[0],
            entityId: entityReferenceSplit[1]
        },
        sequenceNumber: unmarshalledImage.sequenceNumber,
        eventName: unmarshalledImage.eventName,
        timestamp: unmarshalledImage.timestamp,
        payload: JSON.parse(unmarshalledImage.payload)
    };
}

async function processRecord(record) {
    if (record.eventName !== 'INSERT') return;

    const newImage = record.dynamodb?.NewImage;
    if (!newImage) return;

    const unmarshalledImage = unmarshall(newImage);
    const {entityReference} = unmarshalledImage;

    if (!entityReference) {
        throw new Error('Missing entityReference');
    }

    const topicName = deriveTopicName(entityReference);
    const topicArn = deriveTopicArn(topicName);
    const messageBody = constructMessageBody(unmarshalledImage);

    const command = new PublishCommand({
        TopicArn: topicArn,
        Message: JSON.stringify(messageBody)
    });

    await snsClient.send(command);
}

export const handler = async (event) => {
    const promises = event.Records.map(record => processRecord(record));

    await Promise.all(promises);
};