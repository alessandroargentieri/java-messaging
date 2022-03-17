package com.example.notificationdemo.controllers;

import com.example.notificationdemo.notifications.producers.EndpointEventProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * Consumer class for the {@link EndpointEventProducer}.
 */
@org.springframework.web.bind.annotation.RestController
public class RestController {

    /**
     * Callback API
     *
     * @param messageBody the API payload
     * @return the response object acknoledgement
     */
    @PostMapping("/callback")
    public String callbackAPI(@RequestBody String messageBody) {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println(String.format("Callback-API received this message: %s", messageBody));
        return "OK";
    }
}
