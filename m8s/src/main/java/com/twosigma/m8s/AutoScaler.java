package com.twosigma.m8s;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.InstanceGroupManager;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AutoScaler {
    private static final String APPLICATION_NAME = "AutoScaler";
    private static final long POLL_PERIOD_SECONDS = 10;

    private final ApiClient k8sClient;
    private final Container gkeClient;
    private final Compute gceClient;
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

    public AutoScaler(
            ApiClient k8sClient,
            Container gkeClient,
            Compute gceClient,
            CookAdminClientInterface cookAdminClient) {
        this.k8sClient = k8sClient;
        this.gkeClient = gkeClient;
        this.cookAdminClient = cookAdminClient;
        this.gceClient = gceClient;
    }

    public void run() throws InterruptedException {
        while (true) {
            System.out.println("Enter autoscaler main poll loop");

            try {
                if (this.cookQueueBusy()) {
                    // Only scale up 1 VM at a time for now. No scale down.
                    this.scaleUpNodePool();
                }
            } catch (Exception e) {
                System.out.println("Fail to query cook queue or scale up node");
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
    private void scaleUpNodePool() throws IOException {
        String parent = String.format("projects/%s/locations/%s/clusters/%s", projectId, location, cluster);
        // API reference: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters.nodePools/list
        Container.Projects.Locations.Clusters.NodePools.List listNodePoolsRequest =
                this.gkeClient.projects().locations().clusters().nodePools().list(parent);
        ListNodePoolsResponse listNodePoolsResponse = listNodePoolsRequest.execute();
        List<NodePool> pools = listNodePoolsResponse.getNodePools();

        // TODO: We only consider 1 node pool for now
        NodePool defaultPool = pools.get(0);
        String machineGroupUrl = defaultPool.getInstanceGroupUrls().get(0);

        // Machine group url is like:
        // https://www.googleapis.com/compute/v1/projects/rodrigo-dev/zones/us-central1-a/instanceGroupManagers/gke-m8s-dev-1-default-pool-2daaf601-grp
        Pattern p = Pattern.compile(
                "https://www.googleapis.com/compute/v1/projects/([^/]+)/zones/([^/]+)/instanceGroupManagers/([^/]+)");
        Matcher m = p.matcher(machineGroupUrl);
        if (!m.find()) {
            System.out.println("Machine group of pool " + defaultPool.getName() +
                    " doesn't conform to the right format (" + machineGroupUrl + ")");
            return;
        }

        final String project = m.group(1);
        final String zone = m.group(2);
        final String instanceGroupName = m.group(3);
        Compute.InstanceGroupManagers.Get request = this.gceClient.instanceGroupManagers().get(
                project, zone, instanceGroupName);
        InstanceGroupManager instanceGroup = request.execute();
        int currentTargetSize = instanceGroup.getTargetSize();
        if (instanceGroup.getCurrentActions().getNone() < currentTargetSize) {
            System.out.println("Instance group " + machineGroupUrl + " is still reconciling. Waiting before scaling.");
        }

        // API reference: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters.nodePools/setSize
        String poolName = String.format("projects/%s/locations/%s/clusters/%s/nodePools/%s",
                projectId, location, cluster, defaultPool.getName());
        int newTargetSize = currentTargetSize + 1;
        System.out.println("Resize node pool " + poolName + " from " + currentTargetSize + " to " + newTargetSize +
                " (instance group " + machineGroupUrl + ")");
        SetNodePoolSizeRequest setNodePoolSizeRequest = new SetNodePoolSizeRequest().setNodeCount(newTargetSize);
        Container.Projects.Locations.Clusters.NodePools.SetSize setSizeRequest =
                this.gkeClient.projects().locations().clusters().nodePools().setSize(poolName, setNodePoolSizeRequest);
        //setSizeRequest.execute();
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

    private static GoogleCredential getGoogleCredential(String clientSecretFilename) throws IOException {
        if (clientSecretFilename == null) {
            return GoogleCredential.getApplicationDefault();
        }

        List<String> scopes = new ArrayList<>();
        scopes.add("https://www.googleapis.com/auth/cloud-platform");
        return GoogleCredential.fromStream(new FileInputStream(clientSecretFilename)).createScoped(scopes);
    }

    /**
     * Build a GKE client
     * API Example: https://github.com/google/google-api-java-client-samples/tree/master/compute-engine-cmdline-sample
     */
    private static Container buildGkeClient(String clientSecretFilename) throws Exception {
        GoogleCredential credential = AutoScaler.getGoogleCredential(clientSecretFilename);

        // Create compute engine object for listing instances
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        Container gkeClient = new Container.Builder(
                httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME).build();
        return gkeClient;
    }

    /**
     * Build a GCE client
     * API Example: https://github.com/google/google-api-java-client-samples/tree/master/compute-engine-cmdline-sample
     */
    private static Compute buildGceClient(String clientSecretFilename) throws Exception {
        GoogleCredential credential = AutoScaler.getGoogleCredential(clientSecretFilename);

        // Create compute engine object for listing instances
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        Compute gceClient = new Compute.Builder(
                httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME).build();
        return gceClient;
    }

    public static void main(String[] args) throws Exception {
        CookAdminClientInterface cookAdminClient = new CookAdminClient(
                "http://localhost:8000/sample_cook_queue.txt");
        ApiClient k8sClient = ApiClientBuilder.build("config/m8s-dev-1.yaml");
        Container gkeClient = buildGkeClient("config/client_secrets.json");
        Compute gceClient = buildGceClient("config/client_secrets.json");
        AutoScaler scaler = new AutoScaler(k8sClient, gkeClient, gceClient, cookAdminClient);
        scaler.run();
    }
}
