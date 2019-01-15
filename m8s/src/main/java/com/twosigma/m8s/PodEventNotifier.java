package com.twosigma.m8s;

import org.joda.time.DateTime;

/**
 * Created by rodrigo on 1/15/19.
 */
public interface PodEventNotifier {

    void handlePodStarted(String podName, DateTime firstTimestamp, DateTime lastTimestamp);
    void handlePodFinished(String podName, DateTime firstTimestamp, DateTime lastTimestamp);
    void handlePodKilled(String podName, DateTime firstTimestamp, DateTime lastTimestamp);

}
