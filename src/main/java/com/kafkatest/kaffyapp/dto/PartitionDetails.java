package com.kafkatest.kaffyapp.dto;

import java.util.List;

public class PartitionDetails {
    private int partitionId;
    private int leaderId;
    private List<Integer> replicas;
    private List<Integer> inSyncReplicas;
    private boolean isUnderReplicated;
    private boolean isPreferredLeader;
    private long earliestOffset;
    private long latestOffset;
    private long messageCount;

    public PartitionDetails() {}

    public PartitionDetails(int partitionId, int leaderId, List<Integer> replicas,
                            List<Integer> inSyncReplicas, boolean isUnderReplicated,
                            boolean isPreferredLeader, long earliestOffset,
                            long latestOffset, long messageCount) {
        this.partitionId = partitionId;
        this.leaderId = leaderId;
        this.replicas = replicas;
        this.inSyncReplicas = inSyncReplicas;
        this.isUnderReplicated = isUnderReplicated;
        this.isPreferredLeader = isPreferredLeader;
        this.earliestOffset = earliestOffset;
        this.latestOffset = latestOffset;
        this.messageCount = messageCount;
    }

    // Getters and Setters
    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public List<Integer> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<Integer> replicas) {
        this.replicas = replicas;
    }

    public List<Integer> getInSyncReplicas() {
        return inSyncReplicas;
    }

    public void setInSyncReplicas(List<Integer> inSyncReplicas) {
        this.inSyncReplicas = inSyncReplicas;
    }

    public boolean isUnderReplicated() {
        return isUnderReplicated;
    }

    public void setUnderReplicated(boolean underReplicated) {
        isUnderReplicated = underReplicated;
    }

    public boolean isPreferredLeader() {
        return isPreferredLeader;
    }

    public void setPreferredLeader(boolean preferredLeader) {
        isPreferredLeader = preferredLeader;
    }

    public long getEarliestOffset() {
        return earliestOffset;
    }

    public void setEarliestOffset(long earliestOffset) {
        this.earliestOffset = earliestOffset;
    }

    public long getLatestOffset() {
        return latestOffset;
    }

    public void setLatestOffset(long latestOffset) {
        this.latestOffset = latestOffset;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }
}
