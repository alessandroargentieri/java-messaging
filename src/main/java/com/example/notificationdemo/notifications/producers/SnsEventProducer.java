package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.EventProducer;
import com.example.notificationdemo.notifications.NotificationException;
import com.example.notificationdemo.utils.Properties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sns.model.*;

import javax.naming.OperationNotSupportedException;
import java.net.URI;
import java.net.URISyntaxException;

// https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-simple-notification-service.html

/**
 * This class emits a {@link EventProducer} by publishing a message
 * on a AWS Simple Notification Service (SNS).
 * It automatically creates an AWS SNS Topic (if it doesn't exist yet)
 * and publish the notification.
 *
 * @param <T> the body of the message passed as a JSON String
 */
public class SnsEventProducer<T> implements EventProducer<T> {

    private final String eventName;
    private SnsClient snsClient;
    private String topicArn;
    private ObjectMapper mapper = new ObjectMapper();

    private final static String SNS_CREATION_NOT_ALLOWED = "the application is not allowed to create a new AWS SNS topic";

    /**
     * Returns a new {@link SnsEventProducer}. It tries to create the AWS SNS topic
     * (if the app has been allowed with the aws.enable.sns.create property set to 'true').
     *
     * @param eventName the name of the event to be sent on the channel
     * @return the SnsChannel
     * @throws URISyntaxException
     * @throws OperationNotSupportedException
     */
    public static SnsEventProducer createProducer(String eventName) throws URISyntaxException, OperationNotSupportedException {
        SnsClient snsClient = snsClient();
        return new SnsEventProducer(eventName, snsClient(), createSNSTopic(snsClient, eventName +"-sns"));
    }

    /**
     * Returns a new {@link SnsEventProducer}.
     *
     * @param eventName the name of the event to be sent on the channel
     * @param topicArn the existing topic arn to be attached to
     * @return the SnsChannel
     * @throws URISyntaxException
     */
    public static SnsEventProducer createProducer(String eventName, String topicArn) throws URISyntaxException {
        return new SnsEventProducer(eventName, snsClient(), topicArn);
    }

    private SnsEventProducer(String eventName, SnsClient snsClient, String topicArn) {
        this.eventName = eventName;
        this.snsClient = snsClient;
        this.topicArn = topicArn;
    }

    /**
     * Returns the event name.
     *
     * @return the event name
     */
    public String getEventName() {
        return this.eventName;
    }

    /**
     * Issues the given object on the SNS channel.
     *
     * @param body the object to be sent as payload
     * @throws NotificationException
     */
    @Override
    public void issue(T body) throws NotificationException {
        if (body == null) throw new NotificationException("Body is null");
        if (this.snsClient == null)  throw new NotificationException("SnsClient is null");

        try {
            pubTopic(snsClient, mapper.writeValueAsString(body), topicArn);
        } catch (JsonProcessingException e) {
            throw new NotificationException(e.getMessage());
        }
    }

    /**
     * Returns the SNS topic arn.
     *
     * @return the topic arn
     */
    public String getTopicArn() {
        return this.topicArn;
    }

    /**
     * Returns the AWS SNS Client.
     *
     * @return the sns client
     */
    public SnsClient getSnsClient() {
        return snsClient;
    }

    private static SnsClient snsClient() throws URISyntaxException {
        SnsClientBuilder snsClientBuilder = SnsClient.builder();
        /* overrides the aws endpoint to the localstack endpoint (in place of the default AWS endpoint of the SnsClient)
           if the aws.endpoint property is specified in the application.properties file */
        if (Properties.get("aws.endpoint") != null) {
            snsClientBuilder.endpointOverride(new URI(Properties.get("aws.endpoint")));
        }
        return snsClientBuilder.build();
    }

    private static String createSNSTopic(SnsClient snsClient, String topicName) throws OperationNotSupportedException {
        if (Boolean.FALSE.equals(Boolean.parseBoolean(Properties.get("aws.enable.sns.create")))) {
            throw new OperationNotSupportedException(SNS_CREATION_NOT_ALLOWED);
        }
        try {
            CreateTopicRequest request = CreateTopicRequest.builder()
                    .name(topicName)
                    .build();
            CreateTopicResponse result = snsClient.createTopic(request);
            return result.topicArn();
        } catch (SnsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    private void pubTopic(SnsClient snsClient, String message, String topicArn) {
        try {
            PublishRequest request = PublishRequest.builder()
                    .message(message)
                    .topicArn(topicArn)
                    .build();

            PublishResponse result = snsClient.publish(request);
            System.out.println(result.messageId() + " Message sent. Status is " + result.sdkHttpResponse().statusCode());

        } catch (SnsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

}
