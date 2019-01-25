package com.twosigma.m8s;

import io.kubernetes.client.models.V1Event;
import io.kubernetes.client.models.V1ObjectReference;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.util.Watch;
import org.joda.time.DateTime;

/**
 * Created by rodrigo on 1/15/19.
 */

public class EventsWatcher implements Runnable {

    private Watch<V1Event> eventsWatch;
    private PodEventNotifier notifier;

    public EventsWatcher(Watch<V1Event> eventsWatch, PodEventNotifier notifier) {
        this.eventsWatch = eventsWatch;
        this.notifier = notifier;
    }

    public void run() {
        while(this.eventsWatch.hasNext()) {
            V1Event event = this.eventsWatch.next().object;
            V1ObjectReference involvedObject = event.getInvolvedObject();
            if (involvedObject == null) {
                System.out.println("involved object is null?!?!");
                continue;
            }
            String kind = involvedObject.getKind();
            if (kind == null) {
                System.out.println("Object doesn't have a kind!?!");
                System.out.println(involvedObject);
                continue;
            }
            if(kind.toLowerCase().equals("pod")) {
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
                } else {
                    System.out.println("Don't know how to handle " + reason);
                }
            } else {
                System.out.println("not pod object... " + event.getReason());
            }
        }
        try {
            Thread.currentThread().sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
