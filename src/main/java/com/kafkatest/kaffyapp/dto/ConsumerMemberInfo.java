package com.kafkatest.kaffyapp.dto;

import java.util.List;

public class ConsumerMemberInfo {
    private String memberId;
    private String instanceId;
    private String clientId;
    private String host;
    private List<Integer> assignedPartitions;

    public ConsumerMemberInfo() {}

    public ConsumerMemberInfo(String memberId, String instanceId, String clientId,
                              String host, List<Integer> assignedPartitions) {
        this.memberId = memberId;
        this.instanceId = instanceId;
        this.clientId = clientId;
        this.host = host;
        this.assignedPartitions = assignedPartitions;
    }

    // Getters and Setters
    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public List<Integer> getAssignedPartitions() {
        return assignedPartitions;
    }

    public void setAssignedPartitions(List<Integer> assignedPartitions) {
        this.assignedPartitions = assignedPartitions;
    }
}
