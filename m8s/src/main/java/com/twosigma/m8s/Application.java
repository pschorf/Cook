package com.twosigma.m8s;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by rodrigo on 1/15/19.
 */
public class Application {

    public static void main(String[] args) throws IOException, ApiException, InterruptedException {

        // Create an API client to talk to a k8s cluster. Here we pass a config file,
        // we could easily use this to talk to more than 1 gke cluster
        ApiClient apiClient = ApiClientBuilder.build("config/m8s-dev-1.yaml");
        M8s m8 = new M8s(apiClient);

        // Start polling pod events, atm we only care about pods started and killed.
        m8.pollPodEvents(new PodEventNotifier() {
            public void handlePodStarted(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp) {
                System.out.println(String.format("Pod %s started with message [%s] at (first) %s (last) %s",
                        podName, message, firstTimestamp, lastTimestamp));
            }

            public void handlePodFinished(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp) {
                System.out.println(String.format("Pod %s finished with message [%s] at (first) %s (last) %s",
                        podName, message, firstTimestamp, lastTimestamp));
            }

            public void handlePodKilled(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp) {
                System.out.println(String.format("Pod %s killed with message [%s] at (first) %s (last) %s",
                        podName, message, firstTimestamp, lastTimestamp));
            }

            public void handlePodFailed(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp) {
                System.out.println(String.format("Pod %s failed with message [%s] at (first) %s (last) %s",
                        podName, message, firstTimestamp, lastTimestamp));
            }

            public void handlePodFailedScheduling(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp) {
                System.out.println(String.format("Pod %s failed scheduling with message [%s] at (first) %s (last) %s",
                        podName, message, firstTimestamp, lastTimestamp));
            }
        });

        Thread.currentThread().sleep(3000);
        String uuid = UUID.randomUUID().toString();
        m8.startPod(0.5, 128, "nginx:latest", "sleep 60", uuid);
    }

}
