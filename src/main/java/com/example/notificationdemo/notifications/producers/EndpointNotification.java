package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.Notification;
import com.example.notificationdemo.notifications.NotificationException;
import org.springframework.http.HttpMethod;
import org.springframework.lang.Nullable;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URL;

/**
 * This class implements a @{link Notification} through an HTTP/S call to an Endpoint.
 * @param <T> the body of the message to be sent over HTTP/S.
 */
public class EndpointNotification<T> implements Notification<T> {

    private String id;
    private Endpoint endpoint;

    public EndpointNotification (final String id, final Endpoint endpoint) {
        this.id = id;
        this.endpoint = endpoint;
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public void issue(T body) throws NotificationException {
        if (body == null || endpoint == null || endpoint.getUrl() == null) {
            throw new NotificationException("Body cannot be null");
        }
        new RestTemplate().postForObject(endpoint.getUrl(),body, String.class);
    }


    /**
     * Inner class representing the endpoint to be called as a notification callback.
     */
    public static class Endpoint {
        private URL url;
        private HttpMethod method;
        private Headers headers;

        public Endpoint(URL url) {
            this.url = url;
        }

        public String getUrl() {
            return url.toString();
        }
    }

}
