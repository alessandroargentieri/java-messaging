package com.example.notificationdemo.notifications.consumers;

import com.example.notificationdemo.notifications.producers.SnsNotification;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sns.model.SubscribeResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * This class acts as a consumer for the {@link SnsNotification}.
 * It creates a queue by specifying the notification id and the topicArn of the related AWS SNS service.
 * The new queue is then subscribed to the given SNS Topic in order to receive and buffer all the incoming messages.
 * Every SqsConsumer instance for a specific notification id has its own copy of the messages.
 */
public class SqsConsumer {

    private SqsClient sqsClient;
    private String sqsEndpoint;
    private static Integer queueNumber = -1;

    // it can be used in the same application to recycle the SnsClient and to automatically get the other inputs
    public SqsConsumer(final SnsNotification producer) throws URISyntaxException {
        this(producer.id(), producer.getTopicArn(), producer.getSnsClient());
    }

    public SqsConsumer(String id, String topicArn) throws URISyntaxException {
        this.sqsClient = sqsClient();  // creates a new SqsClient
        queueNumber++;
        this.sqsEndpoint = createQueue(sqsClient, id+"sqs"+queueNumber+"");
        subscribeToTopic(snsClient(), topicArn, sqsEndpoint);
    }

    private SqsConsumer(String id, String topicArn, final SnsClient snsClient) throws URISyntaxException {
        this.sqsClient = sqsClient;  // reuses the sqsClient given in input
        queueNumber++;
        this.sqsEndpoint = createQueue(sqsClient, id+"sqs"+queueNumber+"");
        subscribeToTopic(snsClient(), topicArn, sqsEndpoint);
    }

    private SqsClient sqsClient() throws URISyntaxException {
        return SqsClient.builder().endpointOverride(new URI("http://localhost:4566")).build();
    }

    private SnsClient snsClient() throws URISyntaxException {
        return SnsClient.builder().endpointOverride(new URI("http://localhost:4566")).build();
    }

    private String createQueue(SqsClient sqsClient, String queueName) {
        try {
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();

            sqsClient.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            return queueUrl;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    /**
     * Reads the Messages from the SQS queue and removes them from the queue.
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

}