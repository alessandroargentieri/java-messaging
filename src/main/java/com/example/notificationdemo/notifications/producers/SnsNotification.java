package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.Notification;
import com.example.notificationdemo.notifications.NotificationException;
import com.example.notificationdemo.utils.Properties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sns.model.*;

import java.net.URI;
import java.net.URISyntaxException;

// https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-simple-notification-service.html

/**
 * This class emits a {@link Notification} by publishing a message
 * on a AWS Simple Notification Service (SNS).
 * It automatically creates an AWS SNS Topic (if it doesn't exist yet)
 * and publish the notification.
 * @param <T> the body of the message passed as a JSON String
 */
public class SnsNotification<T> implements Notification<T> {

    private String id;
    private SnsClient snsClient;
    private String topicArn;
    private ObjectMapper mapper = new ObjectMapper();

    public SnsNotification(String id) throws URISyntaxException {
        this.id = id;
        this.snsClient = snsClient();
        this.topicArn = createSNSTopic(snsClient, id+"-sns");
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public void issue(T body) throws NotificationException {
        try {
            pubTopic(snsClient, mapper.writeValueAsString(body), topicArn);
        } catch (JsonProcessingException e) {
            throw new NotificationException(e.getMessage());
        }
    }

    public String getTopicArn() {
        return this.topicArn;
    }

    public SnsClient getSnsClient() {
        return snsClient;
    }

    public void setSnsClient(SnsClient snsClient) {
        this.snsClient = snsClient;
    }

    private SnsClient snsClient() throws URISyntaxException {
        SnsClientBuilder snsClientBuilder = SnsClient.builder();
        /* overrides the aws endpoint to the localstack endpoint (in place of the default AWS endpoint of the SnsClient)
           if the aws.endpoint property is specified in the application.properties file */
        if (Properties.get("aws.endpoint") != null) {
            snsClientBuilder.endpointOverride(new URI(Properties.get("aws.endpoint")));
        }
        return snsClientBuilder.build();
    }

    private String createSNSTopic(SnsClient snsClient, String topicName ) {
        CreateTopicResponse result = null;
        try {
            CreateTopicRequest request = CreateTopicRequest.builder()
                    .name(topicName)
                    .build();

            result = snsClient.createTopic(request);
            return result.topicArn();
        } catch (SnsException e) {

            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
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