package com.kafkatest.kaffyapp.dto;

public class BrokerInfo {
    private int brokerId;
    private String hostname;
    private int port;
    private String rack;
    private boolean isController;
    private int partitionCount;
    private double partitionPercentage;

    public BrokerInfo() {}

    public BrokerInfo(int brokerId, String hostname, int port, String rack,
                      boolean isController, int partitionCount, double partitionPercentage) {
        this.brokerId = brokerId;
        this.hostname = hostname;
        this.port = port;
        this.rack = rack;
        this.isController = isController;
        this.partitionCount = partitionCount;
        this.partitionPercentage = partitionPercentage;
    }

    // Getters and Setters
    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getRack() {
        return rack;
    }

    public void setRack(String rack) {
        this.rack = rack;
    }

    public boolean isController() {
        return isController;
    }

    public void setController(boolean controller) {
        isController = controller;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public double getPartitionPercentage() {
        return partitionPercentage;
    }

    public void setPartitionPercentage(double partitionPercentage) {
        this.partitionPercentage = partitionPercentage;
    }

    // Alias methods for template compatibility
    public int getId() {
        return brokerId;
    }

    public String getHost() {
        return hostname;
    }
}
