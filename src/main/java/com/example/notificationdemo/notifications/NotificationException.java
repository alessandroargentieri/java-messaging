package com.example.notificationdemo.notifications;

/**
 * NotificationException acts as a wrapper for all the possible exceptions thrown by the various implementations.
 */
public class NotificationException extends Exception {

    public NotificationException(String message) {
        super(message);
    }
}
