package com.example.notificationdemo.notifications.consumers;

import com.example.notificationdemo.notifications.producers.SnsChannel;
import com.example.notificationdemo.utils.ContinuousJob;
import com.example.notificationdemo.utils.Properties;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sns.model.SubscribeResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.naming.OperationNotSupportedException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.function.Consumer;

/**
 * This class acts as a consumer for the {@link SnsChannel}.
 * It creates a queue by specifying the notification id and the topicArn of the related AWS SNS service.
 * The new queue is then subscribed to the given SNS Topic in order to receive and buffer all the incoming messages.
 * Every SqsConsumer instance for a specific notification id has its own copy of the messages.
 * It can be started as a {@link ContinuousJob}.
 */
public class SqsConsumer extends ContinuousJob {

    private String eventName;
    private SqsClient sqsClient;
    private String sqsEndpoint;
    private Consumer<Message> onReadConsumer;
    private static Integer queueNumber = -1;

    private final static String SQS_CREATION_NOT_ALLOWED = "the application is not allowed to create a new AWS SQS queue";

    /**
     * Creates a new instance of a {@link SqsConsumer}.
     * It tries to create a new SQS queue
     * (if the app has been allowed with the aws.enable.sqs.create property set to 'true').
     * The queue is subscribed to the given SNS topic.
     *
     * @param eventName the name of the event
     * @param topicArn the SNS topic arn
     * @return the SqsConsumer
     * @throws URISyntaxException
     * @throws OperationNotSupportedException
     */
    public static SqsConsumer create(String eventName, String topicArn) throws URISyntaxException, OperationNotSupportedException {
        queueNumber++;
        SqsClient sqsClient = sqsClient();
        String sqsEndpoint = createQueue(sqsClient, eventName+"-sqs"+queueNumber+"");
        return new SqsConsumer(eventName, snsClient(), sqsClient, topicArn, sqsEndpoint);
    }

    /**
     * Creates a new instance of a {@link SqsConsumer}.
     * The queue is subscribed to the given SNS topic.
     *
     * @param eventName the name of the event
     * @param topicArn the SNS topic arn
     * @param sqsEndpoint the endpoint of an existing SQS queue
     * @return the SqsConsumer
     * @throws URISyntaxException
     * @throws OperationNotSupportedException
     */
    public static SqsConsumer create(String eventName, String topicArn, String sqsEndpoint) throws URISyntaxException, OperationNotSupportedException {
        SqsClient sqsClient = sqsClient();
        return new SqsConsumer(eventName, snsClient(), sqsClient, topicArn, sqsEndpoint);
    }

    /**
     * Creates a new instance of a {@link SqsConsumer} from a given {@link SnsChannel}
     * It tries to create a new SQS queue
     * (if the app has been allowed with the aws.enable.sqs.create property set to 'true').
     * The queue is subscribed to the SNS topic fetched from given the SnsChannel.
     *
     * @param snsChannel the producer element from which to initialize the consumer
     * @return the SqsConsumer
     * @throws URISyntaxException
     * @throws OperationNotSupportedException
     */
    public static SqsConsumer createFromProducer(final SnsChannel snsChannel) throws URISyntaxException, OperationNotSupportedException {
        queueNumber++;
        SqsClient sqsClient = sqsClient();
        String sqsEndpoint = createQueue(sqsClient, snsChannel.getEventName()+"-sqs"+queueNumber+"");
        return new SqsConsumer(snsChannel.getEventName(), snsChannel.getSnsClient(), sqsClient, snsChannel.getTopicArn(), sqsEndpoint);
    }

    /**
     * Creates a new instance of a {@link SqsConsumer} from a given {@link SnsChannel}
     * The queue is subscribed to the SNS topic fetched from the given SnsChannel.
     *
     * @param snsChannel the producer element from which to initialize the consumer
     * @param sqsEndpoint the endpoint of an existing AWS SQS queue
     * @return the SqsConsumer
     * @throws URISyntaxException
     * @throws OperationNotSupportedException
     */
    public static SqsConsumer createFromProducer(final SnsChannel snsChannel, String sqsEndpoint) throws URISyntaxException, OperationNotSupportedException {
        SqsClient sqsClient = sqsClient();
        return new SqsConsumer(snsChannel.getEventName(), snsChannel.getSnsClient(), sqsClient(), snsChannel.getTopicArn(), sqsEndpoint);
    }

    private SqsConsumer(String eventName, SnsClient snsClient, SqsClient sqsClient, String topicArn, String sqsEndpoint) throws URISyntaxException, OperationNotSupportedException {
        this.eventName = eventName;
        this.sqsClient = sqsClient;
        this.sqsEndpoint = sqsEndpoint;
        subscribeToTopic(snsClient, topicArn, sqsEndpoint);
    }

    private static SqsClient sqsClient() throws URISyntaxException {
        return (Properties.get("aws.endpoint") != null)
                ? SqsClient.builder().endpointOverride(new URI(Properties.get("aws.endpoint"))).build()
                : SqsClient.builder().build();
    }

    private static SnsClient snsClient() throws URISyntaxException {
        return (Properties.get("aws.endpoint") != null)
                ? SnsClient.builder().endpointOverride(new URI(Properties.get("aws.endpoint"))).build()
                : SnsClient.builder().build();
    }

    private static String createQueue(SqsClient sqsClient, String queueName) throws OperationNotSupportedException {
        if (Boolean.FALSE.equals(Boolean.parseBoolean(Properties.get("aws.enable.sqs.create")))) {
            throw new OperationNotSupportedException(SQS_CREATION_NOT_ALLOWED);
        }
        try {
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            sqsClient.createQueue(createQueueRequest);

            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(getQueueUrlRequest);

            return getQueueUrlResponse.queueUrl();
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    /**
     * Reads the Messages from the SQS queue and removes them from the queue.
     *
     * @return the list of the messages
     */
    public List<Message> readMessages() {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(this.sqsEndpoint)
                    .maxNumberOfMessages(5)
                    .build();
            List<Message> messages = this.sqsClient.receiveMessage(receiveMessageRequest).messages();
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                        .queueUrl(this.sqsEndpoint)
                        .receiptHandle(message.receiptHandle())
                        .build();
                this.sqsClient.deleteMessage(deleteMessageRequest);
            }
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return null;
    }

    private void subscribeToTopic(SnsClient snsClient, String topicArn, String sqsEndpoint) {
        try {
            SubscribeRequest request = SubscribeRequest.builder()
                    .protocol("sqs")
                    .endpoint(sqsEndpoint)
                    .returnSubscriptionArn(true)
                    .topicArn(topicArn)
                    .build();

            SubscribeResponse result = snsClient.subscribe(request);
            System.out.println("Subscription ARN: " + result.subscriptionArn() + "\n\n Status is " + result.sdkHttpResponse().statusCode());

        } catch (SnsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    /**
     * Returns the queue endpoint.
     *
     * @return the queue endpoint
     */
    public String getQueueEndpoint() {
        return this.sqsEndpoint;
    }

    /**
     * Specifies the logic to be cyclically repeated.
     */
    @Override
    public void doWork() {
        this.readMessages().forEach(
                message -> {
                    if (message != null) {
                        this.onReadConsumer.accept(message);
                    }
                }
        );
    }

    /**
     * Starts listening and reacting to the messages.
     * Gets a {@link Consumer} to consume the read messages.
     *
     * @param consumer the action to be performed on the read message
     */
    public void onReadStart(Consumer<Message> consumer) {
        // set the callback
        this.onReadConsumer = consumer;
        // start the cyclic execution of the job
        this.start();
    }
}
