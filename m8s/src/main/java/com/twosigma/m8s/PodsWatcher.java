package com.twosigma.m8s;

import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.util.Watch;
import org.joda.time.DateTime;

/**
 * Created by rodrigo on 1/23/19.
 */
public class PodsWatcher implements Runnable {

    private final PodEventNotifier notifier;
    private final Watch<V1Pod> podsWatch;

    public PodsWatcher(Watch<V1Pod> podsWatch, PodEventNotifier notifier) {
        this.podsWatch = podsWatch;
        this.notifier = notifier;
    }

    @Override
    public void run() {
        while (this.podsWatch.hasNext()) {
            V1Pod pod = this.podsWatch.next().object;
            String name = pod.getMetadata().getName();
            String phase = pod.getStatus().getPhase();
            if (phase.equals("Succeeded")) {
                this.notifier.handlePodSucceeded(name, pod.getStatus().getMessage(), DateTime.now(), DateTime.now());
            } else {
                System.out.println("Don't know how to handle phase " + phase + " for pod " + name);
            }
        }
    }
}
