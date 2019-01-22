package com.twosigma.m8s;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import io.kubernetes.client.ApiClient;

import com.google.api.services.container.model.ListNodePoolsResponse;
import com.google.api.services.container.model.NodePool;
import com.google.api.services.container.model.SetNodePoolSizeRequest;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.services.container.Container;
import com.google.api.services.container.ContainerScopes;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

public class AutoScaler {
    private static final String APPLICATION_NAME = "AutoScaler";
    private static final long POLL_PERIOD_SECONDS = 10;

    private final ApiClient k8sClient;
    private final Container gkeClient;
    private final CookAdminClientInterface cookAdminClient;
    private Map<String, Integer> waitingTaskIdToOccurences = new HashMap<>();

    private static HttpTransport httpTransport;
    private static DataStoreFactory dataStoreFactory;
    private static final String projectId = "rodrigo-dev";
    private static final String location = "us-central1-a";
    private static final String cluster = "m8s-dev-1";
    private static final java.io.File DATA_STORE_DIR =
            new java.io.File(System.getProperty("user.home"), ".store/autoscaler");

    /**
     * OAuth 2.0 scopes
     */
    private static final List<String> SCOPES = Arrays.asList(ContainerScopes.CLOUD_PLATFORM);

    /**
     * Global instance of the JSON factory.
     */
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    public AutoScaler(ApiClient k8sClient, Container gkeClient, CookAdminClientInterface cookAdminClient) {
        this.k8sClient = k8sClient;
        this.gkeClient = gkeClient;
        this.cookAdminClient = cookAdminClient;
    }

    public void run() throws Exception {
        while (true) {
            System.out.println("Enter autoscaler main poll loop");

            if (this.cookQueueBusy()) {
                // Only scale up 1 VM at a time for now. No scale down.
                this.scaleUpNodePool();
            }

            System.out.println("Sleeping for " + POLL_PERIOD_SECONDS + " seconds");
            Thread.currentThread().sleep(POLL_PERIOD_SECONDS * 1000);
        }
    }

    private boolean cookQueueBusy() throws Exception {
        List<CookJobInstance> instances = this.cookAdminClient.getCookQueue();
        Map<String, Integer> newWaitingTaskIdToOccurences = new HashMap<>();
        for (CookJobInstance instance : instances) {
            if (instance.getStatus().equals("instance.status/waiting")) {
                newWaitingTaskIdToOccurences.put(instance.getTaskId(), 1);
            }
        }

        for (String taskId : this.waitingTaskIdToOccurences.keySet()) {
            if (newWaitingTaskIdToOccurences.containsKey(taskId)) {
                int newValue = this.waitingTaskIdToOccurences.get(taskId) + newWaitingTaskIdToOccurences.get(taskId);
                newWaitingTaskIdToOccurences.put(taskId, newValue);
                // If an instance is in waiting state twice in a row, consider the queue busy and needs to be scaled up
                if (newValue >= 5) {
                    System.out.println("Task " + taskId + " is busy for " + newValue + " times");
                    this.waitingTaskIdToOccurences = new HashMap<>();
                    return true;
                }
            }
        }

        this.waitingTaskIdToOccurences = newWaitingTaskIdToOccurences;
        return false;
    }

    // Super simple, get current node pool size and increment by 1
    private void scaleUpNodePool() throws Exception {
        String parent = String.format("projects/%s/locations/%s/clusters/%s", projectId, location, cluster);
        // API reference: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters.nodePools/list
        Container.Projects.Locations.Clusters.NodePools.List listNodePoolsRequest =
                this.gkeClient.projects().locations().clusters().nodePools().list(parent);
        ListNodePoolsResponse listNodePoolsResponse = listNodePoolsRequest.execute();
        List<NodePool> pools = listNodePoolsResponse.getNodePools();
        NodePool defaultPool = pools.get(0);

        // TODO: This group size is wrong, need to query the instance group
        int currentPoolSize = defaultPool.size();
        String machineGroupUrl = defaultPool.getInstanceGroupUrls().get(0);

        // API reference: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters.nodePools/setSize
        String poolName = String.format("projects/%s/locations/%s/clusters/%s/nodePools/%s",
                projectId, location, cluster, defaultPool.getName());
        int newPoolSize = currentPoolSize + 1;
        System.out.println("Resize node pool " + poolName + " from " + currentPoolSize + " to " + newPoolSize);
        SetNodePoolSizeRequest setNodePoolSizeRequest = new SetNodePoolSizeRequest().setNodeCount(newPoolSize);
        Container.Projects.Locations.Clusters.NodePools.SetSize setSizeRequest =
                this.gkeClient.projects().locations().clusters().nodePools().setSize(poolName, setNodePoolSizeRequest);
        setSizeRequest.execute();
    }

    /**
     * Authorizes the installed application to access user's protected data.
     */
    private static Credential authorize(String clientSecretFilename) throws Exception {
        // initialize client secrets object
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(
                new FileInputStream(clientSecretFilename)));
        // set up authorization code flow
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                httpTransport, JSON_FACTORY, clientSecrets, SCOPES).setDataStoreFactory(dataStoreFactory)
                .build();
        // authorize
        return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
    }

    /**
     * Build a GKE client
     * API Example: https://github.com/google/google-api-java-client-samples/tree/master/compute-engine-cmdline-sample
     */
    private static Container buildGkeClient(String clientSecretFilename) throws Exception {
        List<String> scopes = new ArrayList<>();
        scopes.add("https://www.googleapis.com/auth/cloud-platform");

        GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream(clientSecretFilename))
                .createScoped(scopes);

        // Create compute engine object for listing instances
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        Container gkeClient = new Container.Builder(
                httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME).build();
        return gkeClient;
    }

    public static void main(String[] args) throws Exception {
        CookAdminClientInterface cookAdminClient = new CookAdminClient(
                "http://localhost:8000/sample_cook_queue.txt");
        ApiClient k8sClient = ApiClientBuilder.build("config/m8s-dev-1.yaml");
        Container gkeClient = buildGkeClient("config/client_secret.json");
        AutoScaler scaler = new AutoScaler(k8sClient, gkeClient, cookAdminClient);
        scaler.run();
    }
}
