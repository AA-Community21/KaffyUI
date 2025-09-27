package com.kafkatest.kaffyapp.controller;

import com.kafkatest.kaffyapp.KaffyAdmin;
import com.kafkatest.kaffyapp.dto.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.ExecutionException;

@RequestMapping("/api")
@RestController
public class KaffyappController {

    @Autowired
    private KaffyAdmin kaffyAdmin;

    @GetMapping("/topics")
    public List<TopicInfo> getTopics (@RequestParam(required = false) String search) {
        return kaffyAdmin.getTopics(search);
    }

    @PostMapping("/createTopic")
    public void createTopics(@RequestBody CreateTopicRequest request) throws ExecutionException, InterruptedException {
        kaffyAdmin.createTopic(request);
    }

    @PostMapping("/deleteTopic")
    public void deleteTopic(@RequestParam String topicName) {
        kaffyAdmin.deleteTopic(topicName);
    }

    @GetMapping("/clusterInfo")
    public ClusterInfo getClusterInfo() throws ExecutionException, InterruptedException {
        return kaffyAdmin.getClusterInfo();
    }

    @GetMapping("/brokerInfo")
    public List<BrokerInfo> getBrokersInfo() {
        return kaffyAdmin.getBrokers();
    }

    @GetMapping("/topics/{topicName}")
    public TopicDetails getTopicDetails(@PathVariable String topicName){

        return kaffyAdmin.getTopicDetails(topicName);
    }

    @GetMapping("/topics/{topicName}/partitions/{partition}/messages/{offset}")
    public ResponseEntity<List<PartitionMessage>> getMessageAtOffset(
            @PathVariable String topicName,
            @PathVariable int partition,
            @PathVariable long offset) {
        try {
            List<PartitionMessage> message = kaffyAdmin.getMessageAtOffset(topicName, partition, offset);
            if (message == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(message);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/topics/{topicName}/partitions/{partition}/messages")
    public ResponseEntity<List<PartitionMessage>> getMessagesInPartition(
            @PathVariable String topicName,
            @PathVariable int partition) {
        try {
            List<PartitionMessage> messages = kaffyAdmin.getMessagesFromATopicPartition(topicName, partition);
            if (messages == null || messages.isEmpty()) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(messages);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/topics/{topicName}/messages")
    public ResponseEntity<List<PartitionMessage>> getMessagesFromTopic(
            @PathVariable String topicName) {
        try {
            List<PartitionMessage> messages = kaffyAdmin.getMessagesFromTopic(topicName);
            if (messages == null || messages.isEmpty()) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(messages);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/send/")
    public ResponseEntity<String> publishMessage(@RequestBody PublishMessageRequest request) {
        try {
            kaffyAdmin.sendMessage(request.getTopicName(), request.getKey(), request.getValue());
            return ResponseEntity.ok("Message published successfully");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Failed to publish message");
        }
    }

    @GetMapping("/listingTopics")
    public ResponseEntity<List<InternalTopicsInfo>> getListing(){
        try {
            List<InternalTopicsInfo> topicListings = kaffyAdmin.getTopicListing();
            return ResponseEntity.ok(topicListings);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
