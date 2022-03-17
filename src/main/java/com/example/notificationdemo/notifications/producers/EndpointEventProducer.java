package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.EventProducer;
import com.example.notificationdemo.notifications.NotificationException;
import org.apache.http.Header;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.net.URL;
import java.util.List;

/**
 * EndpointChannel class implements a {@link EventProducer} through an HTTP/S call to an Endpoint.
 *
 * @param <T> the body of the message to be sent over HTTP/S.
 */
public class EndpointEventProducer<T> implements EventProducer<T> {

    private Endpoint endpoint;

    public EndpointEventProducer(final Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Emits the notification in JSON String format.
     *
     * @param body the object to be sent as payload
     * @throws NotificationException
     */
    @Override
    public void issue(T body) throws NotificationException {
        if (body == null) throw new NotificationException("Body is null");
        if (this.endpoint == null || this.endpoint.getUrl() == null)  throw new NotificationException("Endpoint is null");

        new RestTemplate().postForObject(endpoint.getUrl(),body, String.class);
    }

    /**
     * Inner class representing the endpoint to be called as a notification callback.
     */
    public static class Endpoint {
        private URL url;
        private HttpMethod method;
        private List<Header> headers;

        public Endpoint(URL url) {
            this.url = url;
        }

        public String getUrl() {
            return url.toString();
        }
    }

}
