package com.twosigma.m8s;

import io.kubernetes.client.models.V1Event;
import io.kubernetes.client.util.Watch;

/**
 * Created by rodrigo on 1/15/19.
 */

public class NamespaceEventsWatcher implements Runnable {

    private Watch<V1Event> eventsWatch;

    public NamespaceEventsWatcher(Watch<V1Event> eventsWatch) {
        this.eventsWatch = eventsWatch;
    }

    public void run() {
        System.out.println("watching event");
        while (true) {
            for (Watch.Response<V1Event> v1EventResponse : this.eventsWatch) {
                System.out.println(v1EventResponse.object);
            }
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
