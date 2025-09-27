package com.kafkatest.kaffyapp.dto;

import org.apache.kafka.common.Uuid;

public class InternalTopicsInfo {
    private Uuid topicId;
    private String topicName;

    public InternalTopicsInfo(Uuid topicId, String name) {
        this.topicId = topicId;
        this.topicName = name;
    }

    public Uuid getTopicId() {
        return topicId;
    }

    public void setTopicId(Uuid topicId) {
        this.topicId = topicId;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }
}
