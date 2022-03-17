package com.example.notificationdemo.notifications;

/**
 * Interface representing the generic async event it must be notified to listeners.
 * @param <T> the type of the object it must be notified as body of the message.
 */
public interface EventProducer<T> {

    /**
     * Emits the notification on a given channel
     * @param body the object to be sent as payload
     * @throws NotificationException
     */
    void issue(T body) throws NotificationException;
}
