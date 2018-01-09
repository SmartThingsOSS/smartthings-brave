
## SmartThings Brave SNS #

Implements trace propagation through AWS SNS topics via a message's MessageAttributes.  At this time, only the PublishRequest 
is instrumented.

### Usage

Register the PublishRequestTracingHandler (along with any other request handlers) when building the AmazonSNS client:

    AmazonSNS client = AmazonSNSClientBuilder
        .standard()
        .withRequestHandlers(new PublishRequestTracingHandler(httpTracing.tracing()))
        .build()

Issue a publish request:

    PublishRequest publishRequest = new PublishRequest()
        .withTopicArn(topicArn)
        .withMessage(messageBody)
        
    client.publish(publishRequest)

The resulting published message will have trace and span information in the MessageAttributes 
along with the standard SNS message attributes

    "MessageAttributes": {
        "X-B3-Sampled": {
            "DataType": "String", 
            "StringValue": "1"
        }, 
        "X-B3-ParentSpanId": {
            "DataType": "String", 
            "StringValue": "1493c431c9630d6a"
        },
        "X-B3-TraceId": {
            "DataType": "String", 
            "StringValue": "1493c431c9630d6a"
        }, 
        "X-B3-SpanId": {
            "DataType": "String", 
            "StringValue": "73b3a8a29991455a"
        }, 
    }

### Topic Configuration

By default, SNS will not propagate MessageAttributes to the consumer.  In order for the consumer to receive these,
you will need to configure the consumer's subscription with `RawMessageDelivery`

    aws sns set-subscription-attributes --attribute-name RawMessageDelivery --attribute-value true --subscription-arn arn:aws:sns:us-east-1:1465414804035:e9126059-9eab-4b37-8194-e0d64dfb2045
    
Publishing to an SNS topic is a one way operation, so adding these to the message by itself has little value
unless you configure a subscription with a consumer that is smart enough to read the attributes. If you are consuming the topic 
messages via SQS, [SmartThings Brave SQS](https://github.com/SmartThingsOSS/smartthings-brave/tree/master/brave-sqs) will handle this.   
    
   