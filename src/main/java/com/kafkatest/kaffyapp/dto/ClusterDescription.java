package com.kafkatest.kaffyapp.dto;

import org.apache.kafka.common.Node;

import java.util.Collection;

public class ClusterDescription {
    private String clusterId;
    private Collection<Node> nodes;
    private Node controller;

    public ClusterDescription(String clusterId, Collection<Node> nodes, Node controller) {
        this.clusterId = clusterId;
        this.nodes = nodes;
        this.controller = controller;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public Collection<Node> getNodes() {
        return nodes;
    }

    public void setNodes(Collection<Node> nodes) {
        this.nodes = nodes;
    }

    public Node getController() {
        return controller;
    }

    public void setController(Node controller) {
        this.controller = controller;
    }
}
