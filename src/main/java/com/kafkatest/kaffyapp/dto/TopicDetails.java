package com.kafkatest.kaffyapp.dto;

import java.util.List;
import java.util.Map;

public class TopicDetails {
    private String topicName;
    private int partitionCount;
    private int replicationFactor;
    private Map<String, String> configurations;
    private List<PartitionDetails> partitions;
    private List<ConsumerGroupInfo> consumerGroups;

    public TopicDetails() {}

    public TopicDetails(String topicName, int partitionCount, int replicationFactor,
                        Map<String, String> configurations, List<PartitionDetails> partitions,
                        List<ConsumerGroupInfo> consumerGroups) {
        this.topicName = topicName;
        this.partitionCount = partitionCount;
        this.replicationFactor = replicationFactor;
        this.configurations = configurations;
        this.partitions = partitions;
        this.consumerGroups = consumerGroups;
    }

    // Getters and Setters
    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Map<String, String> getConfigurations() {
        return configurations;
    }

    public void setConfigurations(Map<String, String> configurations) {
        this.configurations = configurations;
    }

    public List<PartitionDetails> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<PartitionDetails> partitions) {
        this.partitions = partitions;
    }

    public List<ConsumerGroupInfo> getConsumerGroups() {
        return consumerGroups;
    }

    public void setConsumerGroups(List<ConsumerGroupInfo> consumerGroups) {
        this.consumerGroups = consumerGroups;
    }
}
