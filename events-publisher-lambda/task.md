1. Help me to write events-publisher-lambda README.md about what lambda index.mjs should do and what is its expectations
   for its infra setup.
2. The lambda is needed for forwarding documents inserted into DynamoDB to SNS (each document is an immutable
   event).
3. Keep in mind that code itself is only to be executed in AWS or Local simulator infrastructures. Not the infrastructure
   setup itself. So we produce no infra code, just the lambda code.
4. We want to use latest Node.js 24.x runtime.
5. What env variables must lambda receive:
   1. APP_NAME (required)
   2. AWS_ENDPOINT (optional) to override AWS endpoint with test endpoints
   3. Ask if more variables are necessary
6. DynamoDB documents details can be found in java io.saga.caravan.event.sourcing.dynamodb.DynamoDbBasedEventStore
7. Target topic name is to be derived by 
   ```javascript
    function deriveTopicArnPrefix(invokedFunctionArn, entityName) {
        const [, partition, , region, accountId] = invokedFunctionArn.split(':');
        return `arn:${partition}:sns:${region}:${accountId}:${APP_NAME}_` + entityName;
    }
    ```
8. Event structure to send into SNS can be found in java io.saga.caravan.event.Event
9. Message attributes to be sent with each SNS message are entityName and eventName as Strings (to add possibility to filter)
10. We should utilize batch publishing (grouped by topic) and batch publishing parallelization in order to speed up performance
11. If batch size is too large, we should publish the messages one by one.
12. If there are poisonous messages (for example it's too large), they should be retried and eventually put into DLQ by AWS itself (no code by lambda to
    place them into DLQ)
13. We want as much work as possible done by AWS.
14. Simplicity, cost, performance, scalability are our values.