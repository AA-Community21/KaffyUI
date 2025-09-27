package com.kafkatest.kaffyapp.dto;

import java.util.List;

public class ConsumerGroupInfo {
    private String groupId;
    private String state;
    private Integer groupEpoch;
    private List<ConsumerMemberInfo> members;

    public ConsumerGroupInfo() {}

    public ConsumerGroupInfo(String groupId, String state, Integer groupEpoch, List<ConsumerMemberInfo> members) {
        this.groupId = groupId;
        this.state = state;
        this.groupEpoch = groupEpoch;
        this.members = members;
    }

    // Getters and Setters
    public String getGroupId() {
        return groupId;
    }

    public Integer getGroupEpoch() {
        return groupEpoch;
    }
    public void setGroupEpoch(Integer groupEpoch) {
        this.groupEpoch = groupEpoch;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public List<ConsumerMemberInfo> getMembers() {
        return members;
    }

    public void setMembers(List<ConsumerMemberInfo> members) {
        this.members = members;
    }
}
