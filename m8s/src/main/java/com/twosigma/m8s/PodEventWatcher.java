package com.twosigma.m8s;

import io.kubernetes.client.models.V1Event;
import io.kubernetes.client.models.V1ObjectReference;
import io.kubernetes.client.util.Watch;

/**
 * Created by rodrigo on 1/15/19.
 */

public class PodEventWatcher implements Runnable {

    private Watch<V1Event> eventsWatch;
    private PodEventNotifier notifier;

    public PodEventWatcher(Watch<V1Event> eventsWatch, PodEventNotifier notifier) {
        this.eventsWatch = eventsWatch;
        this.notifier = notifier;
    }

    public void run() {
        while (true) {
            while(this.eventsWatch.hasNext()) {
                V1Event event = this.eventsWatch.next().object;
                V1ObjectReference involvedObject = event.getInvolvedObject();
                if(involvedObject.getKind().toLowerCase().equals("pod")) {
                    String podName = involvedObject.getName();
                    String reason = event.getReason();
                    if (reason.equals("Killing")) {
                        this.notifier.handlePodKilled(podName);
                    } else if (reason.equals("Started")) {
                        this.notifier.handlePodStarted(podName);
                    }
                }
            }
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
