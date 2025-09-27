package com.kafkatest.kaffyapp.dto;

public class ClusterInfo {

    private String clusterId;
    private String bootstrapServers;
    private int totalTopics;
    private int totalPartitions;
    private double preferredLeaderPercentage;
    private int underReplicatedPartitions;
    private Controller controller;

    public ClusterInfo(String clusterId, String bootstrapServers, int totalTopics, int totalPartitions, double preferredLeaderPercentage, int underReplicatedPartitions, Controller controller) {
        this.clusterId = clusterId;
        this.bootstrapServers = bootstrapServers;
        this.totalTopics = totalTopics;
        this.totalPartitions = totalPartitions;
        this.preferredLeaderPercentage = preferredLeaderPercentage;
        this.underReplicatedPartitions = underReplicatedPartitions;
        this.controller = controller;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public int getTotalTopics() {
        return totalTopics;
    }

    public void setTotalTopics(int totalTopics) {
        this.totalTopics = totalTopics;
    }

    public int getTotalPartitions() {
        return totalPartitions;
    }

    public void setTotalPartitions(int totalPartitions) {
        this.totalPartitions = totalPartitions;
    }

    public double getPreferredLeaderPercentage() {
        return preferredLeaderPercentage;
    }

    public void setPreferredLeaderPercentage(double preferredLeaderPercentage) {
        this.preferredLeaderPercentage = preferredLeaderPercentage;
    }

    public int getUnderReplicatedPartitions() {
        return underReplicatedPartitions;
    }

    public void setUnderReplicatedPartitions(int underReplicatedPartitions) {
        this.underReplicatedPartitions = underReplicatedPartitions;
    }

    public Controller getController() {
        return controller;
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }
}
