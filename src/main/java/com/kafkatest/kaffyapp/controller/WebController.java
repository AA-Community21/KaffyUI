package com.kafkatest.kaffyapp.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class WebController {

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @GetMapping("/topic/{topicName}")
    public String topicDetails() {
        return "index";
    }

    @GetMapping("/messages/{topicName}/{partition}")
    public String partitionMessages() {
        return "index";
    }

    @GetMapping("/message/{topicName}/{partition}/{offset}")
    public String messageDetails() {
        return "index";
    }

    @GetMapping("/create-topic")
    public String createTopic() {
        return "index";
    }
}