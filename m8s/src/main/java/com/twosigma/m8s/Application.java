package com.twosigma.m8s;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;

import java.io.IOException;

/**
 * Created by rodrigo on 1/15/19.
 */
public class Application {

    public static void main(String[] args) throws IOException, ApiException {

        // Create an API client to talk to a k8s cluster. Here we pass a config file,
        // we could easily use this to talk to more than 1 gke cluster
        ApiClient apiClient = ApiClientBuilder.build("config/m8s-dev-1.yaml");
        M8s m8 = new M8s(apiClient);

        // Start polling pod events, atm we only care about pods started and killed.
        m8.pollPodEvents(new PodEventNotifier() {
            public void handlePodStarted(String podName) {
                System.out.println("Pod " + podName + "has started");
            }

            public void handlePodFinished(String podName) {

            }

            public void handlePodKilled(String podName) {
                System.out.println("Pod " + podName + "has been killed");
            }
        });
    }

}
