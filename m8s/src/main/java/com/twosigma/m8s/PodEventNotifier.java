package com.twosigma.m8s;

/**
 * Created by rodrigo on 1/15/19.
 */
public interface PodEventNotifier {

    void handlePodStarted(String podName);
    void handlePodFinished(String podName);
    void handlePodKilled(String podName);

}
