package com.twosigma.m8s;

import com.google.api.services.compute.Compute;
import com.google.api.services.container.Container;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.custom.Quantity;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScalingBenchmark {

    // This only works for 1 node pool per cluster
    public static void main(String[] args) throws Exception {
        ApiClient k8sClient = ApiClientBuilder.build("config/m8s-dev-2.yaml");
        M8s m8sClient = new M8s(k8sClient);

        String clientSecretPath = "config/client_secrets.json";
        Container gkeClient = AutoScaler.buildGkeClient(clientSecretPath);
        Compute gceClient = AutoScaler.buildGceClient(clientSecretPath);
        AutoScaler scaler = new AutoScaler(
                "rodrigo-dev",
                "us-central1-a",
                "standard-cluster-1",
                k8sClient,
                gkeClient,
                gceClient,
                null,
                m8sClient);

        int currentSize = scaler.getNodePoolTargetSize();
        int targetSize = currentSize - 100;

        System.out.println("Scaling from " + currentSize + " to " + targetSize);
        long startTimeMillis = System.currentTimeMillis();
        scaler.scaleNodePool(targetSize);
        while(true) {
            int numReadyNode = m8sClient.listReadyNode().size();
            System.out.println("Num ready node " + numReadyNode + ", waiting for " + targetSize);
            if (numReadyNode == targetSize) {
                break;
            }
            Thread.currentThread().sleep(5000);
        }
        long endTimeMillis = System.currentTimeMillis();
        long durationSecs = (endTimeMillis - startTimeMillis) / 1000;
        // Sample run data:
        // + Scale up 1 node took 87 secs
        // + Scale up 10 nodes took 82/114 secs
        // + Scale up 100 nodes took 111 secs
        // + Scale down 1 node took 188 seconds
        // + Scale down 10 nodes took 173 seconds
        // + Scale down 100 nodes took 239 seconds
        System.out.println("Scaling from " + currentSize + " to " + targetSize + " takes " + durationSecs + " seconds");
    }

}
