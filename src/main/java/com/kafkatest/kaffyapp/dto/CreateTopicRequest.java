package com.kafkatest.kaffyapp.dto;

public class CreateTopicRequest {
    private String topicName;
    private int noOfPartitions;
    private short replicas;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getNoOfPartitions() {
        return noOfPartitions;
    }

    public void setNoOfPartitions(int noOfPartitions) {
        this.noOfPartitions = noOfPartitions;
    }


    public short getReplicas() {
        return replicas;
    }

    public void setReplicas(short replicas) {
        this.replicas = replicas;
    }
}
