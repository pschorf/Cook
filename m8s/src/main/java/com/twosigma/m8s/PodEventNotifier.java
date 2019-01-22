package com.twosigma.m8s;

import org.joda.time.DateTime;

/**
 * Created by rodrigo on 1/15/19.
 */
public interface PodEventNotifier {

    void handlePodStarted(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp);
    void handlePodFinished(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp);
    void handlePodKilled(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp);
    void handlePodFailed(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp);
    void handlePodFailedScheduling(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp);
    void handlePodSucceeded(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp);
}
