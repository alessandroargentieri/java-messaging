package com.example.notificationdemo.notifications;

/**
 * Interface representing the generic async event it must be notified to listeners.
 * @param <T> the type of the object it must be notified as body of the message.
 */
public interface Notification<T> {
    String id();
    void issue(T body) throws NotificationException;
}
