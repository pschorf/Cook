package com.twosigma.m8s;

import io.kubernetes.client.models.V1Event;
import io.kubernetes.client.models.V1ObjectReference;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.util.Watch;
import org.joda.time.DateTime;

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
                    //System.out.println(event);
                    String podName = involvedObject.getName();
                    String reason = event.getReason();
                    String message = event.getMessage();
                    DateTime firstTimestamp = event.getFirstTimestamp();
                    DateTime lastTimestamp = event.getLastTimestamp();
                    if (reason.equals("Killing")) {
                        this.notifier.handlePodKilled(podName, message, firstTimestamp, lastTimestamp);
                    } else if (reason.equals("Started")) {
                        this.notifier.handlePodStarted(podName, message, firstTimestamp, lastTimestamp);
                    } else if (reason.equals("Failed")) {
                        this.notifier.handlePodFailed(podName, message, firstTimestamp, lastTimestamp);
                    } else if (reason.equals("FailedScheduling")) {
                        this.notifier.handlePodFailedScheduling(podName, message, firstTimestamp, lastTimestamp);
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
