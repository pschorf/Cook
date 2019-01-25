package com.twosigma.m8s;

import io.kubernetes.client.models.V1Node;
import io.kubernetes.client.models.V1NodeCondition;
import io.kubernetes.client.util.Watch;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by rodrigo on 1/23/19.
 */
public class NodesWatcher implements Runnable {

    private final Watch<V1Node> nodesWatch;
    private final NodeEventNotifier notifier;

    private Set<String> knownNodes = new HashSet<>();

    public NodesWatcher(Watch<V1Node> nodesWatch, NodeEventNotifier notifier) {
        this.nodesWatch = nodesWatch;
        this.notifier = notifier;
    }

    @Override
    public void run() {
        while(nodesWatch.hasNext()) {
            V1Node node = nodesWatch.next().object;
            String nodeName = node.getMetadata().getName();
            if (!knownNodes.contains(nodeName)) {
                for (V1NodeCondition condition : node.getStatus().getConditions()) {
                    if (condition.getType().toLowerCase().equals("ready") && Boolean.valueOf(condition.getStatus())) {
                        knownNodes.add(nodeName);
                        this.notifier.handleNodeUp(nodeName);
                    }
                }
            }
        }
    }
}

