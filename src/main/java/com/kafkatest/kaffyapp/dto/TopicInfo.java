package com.kafkatest.kaffyapp.dto;

public class TopicInfo {
    private String topicName;
    private int partitionCount;
    private double preferredPercentage;
    private int underReplicatedPartitions;
    private boolean hasCustomConfig;

    public TopicInfo() {}

    public TopicInfo(String topicName, int partitionCount, double preferredPercentage,
                     int underReplicatedPartitions, boolean hasCustomConfig) {
        this.topicName = topicName;
        this.partitionCount = partitionCount;
        this.preferredPercentage = preferredPercentage;
        this.underReplicatedPartitions = underReplicatedPartitions;
        this.hasCustomConfig = hasCustomConfig;
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

    public double getPreferredPercentage() {
        return preferredPercentage;
    }

    public void setPreferredPercentage(double preferredPercentage) {
        this.preferredPercentage = preferredPercentage;
    }

    public int getUnderReplicatedPartitions() {
        return underReplicatedPartitions;
    }

    public void setUnderReplicatedPartitions(int underReplicatedPartitions) {
        this.underReplicatedPartitions = underReplicatedPartitions;
    }

    public boolean isHasCustomConfig() {
        return hasCustomConfig;
    }

    public void setHasCustomConfig(boolean hasCustomConfig) {
        this.hasCustomConfig = hasCustomConfig;
    }
}
