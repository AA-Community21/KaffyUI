package com.kafkatest.kaffyapp.dto;

import java.util.Date;
import java.util.Map;

public class PartitionMessage {
    private long offset;
    private String key;
    private String value;
    private Date timestamp;
    private Map<String, String> headers;
    private int partition;
    private String topic;

    public PartitionMessage() {}

    public PartitionMessage(long offset, String key, String value, Date timestamp,
                            Map<String, String> headers, int partition, String topic) {
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = headers;
        this.partition = partition;
        this.topic = topic;
    }

    // Getters and Setters
    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
