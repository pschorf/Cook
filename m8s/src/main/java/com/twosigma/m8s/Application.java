package com.twosigma.m8s;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.custom.Quantity;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Map;
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

        m8.pollNodeEvents(new NodeEventNotifier() {
            @Override
            public void handleNodeUp(String nodeName) {
                System.out.println("Node with name " + nodeName + " came up");
            }
        });

        // Start polling pod events, atm we only care about pods started and killed.
        m8.pollPodEvents("default", new PodEventNotifier() {
            public void handlePodStarted(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp) {
                System.out.println(String.format("Pod %s started with message [%s] at (first) %s (last) %s",
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

            @Override
            public void handlePodSucceeded(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp) {
                System.out.println(String.format("Pod %s succeeded with message [%s] at (first) %s (last) %s",
                        podName, message, firstTimestamp, lastTimestamp));
            }

            @Override
            public void handlePodRunning(String podName, String message, DateTime firstTimestamp, DateTime lastTimestamp) {
                System.out.println(String.format("Pod %s running with message [%s] at (first) %s (last) %s",
                        podName, message, firstTimestamp, lastTimestamp));
            }
        });

        // Create dummy container
        String uuid = UUID.randomUUID().toString();
        System.out.println("Starting pod with uuid " + uuid);
        m8.startPod("rodrigo", 0.1, 128, "nginx:latest", "sleep 5", "gke-m8s-dev-1-default-pool-2daaf601-x9g5", uuid);

        //m8.populateOrRefreshKerberosTicket("rodrigo");

        // Pull available resources
        // Each map entry is like
        // ephemeral-storage=Quantity{number=47093746742, format=DECIMAL_SI},
        // memory=Quantity{number=2242727936, format=BINARY_SI},
        // cpu=Quantity{number=0.740, format=DECIMAL_SI},
        // hugepages-2Mi=Quantity{number=0, format=DECIMAL_SI},
        // pods=Quantity{number=110, format=DECIMAL_SI}}

        //for (Map.Entry<String, Map<String, Quantity>> stringMapEntry : m8.getAvailableResources().entrySet()) {
        //    System.out.println(stringMapEntry);
        //}
    }

}
