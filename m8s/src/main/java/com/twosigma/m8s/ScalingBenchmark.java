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

        boolean readyNode = false;
        int currentSize = scaler.getNodePoolTargetSize();
        int targetSize = currentSize - 100;

        System.out.println("Scaling from " + currentSize + " to " + targetSize);
        long startTimeMillis = System.currentTimeMillis();
        scaler.scaleNodePool(targetSize);
        while(true) {
            int numNode = m8sClient.listNode(readyNode).size();
            System.out.println("Num node (isReady:" + readyNode + ") " + numNode + ", waiting for " + targetSize);
            if (numNode == targetSize) {
                break;
            }
            Thread.currentThread().sleep(5000);
        }
        long endTimeMillis = System.currentTimeMillis();
        long durationSecs = (endTimeMillis - startTimeMillis) / 1000;
        // Sample run data for isReady=true:
        // + Scale up 1 node took 87 secs
        // + Scale up 10 nodes took 82/114 secs
        // + Scale up 100 nodes took 111 secs
        // + Scale down 1 node took 188 seconds
        // + Scale down 10 nodes took 173 seconds
        // + Scale down 100 nodes took 239 seconds

        // Sample run data for isReady=false:
        // + Scale up 1 node took 76 secs
        // + Scale up 10 nodes took 54 secs
        // + Scale up 100 nodes took 90 secs
        // + Scale down 1 node took 147 seconds
        // + Scale down 10 nodes took 158 seconds
        // + Scale down 100 nodes took 261 seconds
        System.out.println("Scaling from " + currentSize + " to " + targetSize + " takes " + durationSecs + " seconds");
    }

}
