package com.twosigma.m8s;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.InstanceGroupManager;
import com.google.api.services.compute.model.InstanceTemplate;
import com.google.api.services.compute.model.MachineType;
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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AutoScaler {
    private static final String APPLICATION_NAME = "AutoScaler";
    private static final long POLL_PERIOD_SECONDS = 10;
    private static final long MIN_TIME_BETWEEN_SCALE_UP_SECONDS = 60;

    private final ApiClient k8sClient;
    private final Container gkeClient;
    private final Compute gceClient;
    private final CookAdminClientInterface cookAdminClient;
    private long lastScaleUpTimeMilliseconds;

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

    private class Resource {
        public int numCpu;
        public int numMemMB;
        public Resource() {
            this.numCpu = 0;
            this.numMemMB = 0;
        }
    }

    public AutoScaler(
            ApiClient k8sClient,
            Container gkeClient,
            Compute gceClient,
            CookAdminClientInterface cookAdminClient) {
        this.k8sClient = k8sClient;
        this.gkeClient = gkeClient;
        this.cookAdminClient = cookAdminClient;
        this.gceClient = gceClient;
        this.lastScaleUpTimeMilliseconds = 0;
    }

    public void run() throws InterruptedException {
        while (true) {
            System.out.println("Sleeping for " + POLL_PERIOD_SECONDS + " seconds");
            Thread.currentThread().sleep(POLL_PERIOD_SECONDS * 1000);
            System.out.println("Enter autoscaler main poll loop");

            if (this.lastScaleUpTimeMilliseconds + MIN_TIME_BETWEEN_SCALE_UP_SECONDS * 1000
                    > System.currentTimeMillis()) {
                System.out.println("Last scale up too recent, skip loop");
                continue;
            }

            Resource resourceToBeScaledUp = null;
            try {
                resourceToBeScaledUp = this.cookQueueBusy();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Fail to query cook queue");
                continue;
            }

            if (resourceToBeScaledUp == null ||
                    (resourceToBeScaledUp.numCpu == 0 && resourceToBeScaledUp.numMemMB == 0)) {
                System.out.println("No scale up needed");
                continue;
            }

            try {
                System.out.println("Need to scale up " + resourceToBeScaledUp.numCpu + " cpus "
                        + resourceToBeScaledUp.numMemMB + " mem MBs");
                if (this.scaleUpNodePool(resourceToBeScaledUp)) {
                    this.lastScaleUpTimeMilliseconds = System.currentTimeMillis();
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Fail to scale up node");
                continue;
            }
        }
    }

    private Resource cookQueueBusy() throws Exception {
        List<CookJob> jobs = this.cookAdminClient.getCookQueue();
        if (jobs.isEmpty()) {
            return null;
        }

        Resource resourceToBeScaledUp = new Resource();
        for (CookJob job : jobs) {
            resourceToBeScaledUp.numCpu += job.getNumCpu();
            resourceToBeScaledUp.numMemMB += job.getNumMemMB();
        }
        return resourceToBeScaledUp;
    }

    // Super simple, get current node pool size and increment by 1
    private boolean scaleUpNodePool(Resource resourceToBeScaledUp) throws IOException {
        String parent = String.format("projects/%s/locations/%s/clusters/%s", projectId, location, cluster);
        // API reference: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters.nodePools/list
        Container.Projects.Locations.Clusters.NodePools.List listNodePoolsRequest =
                this.gkeClient.projects().locations().clusters().nodePools().list(parent);
        ListNodePoolsResponse listNodePoolsResponse = listNodePoolsRequest.execute();
        List<NodePool> pools = listNodePoolsResponse.getNodePools();

        // TODO: We only consider 1 node pool for now
        final NodePool defaultPool = pools.get(0);

        // Machine group url is like:
        // https://www.googleapis.com/compute/v1/projects/rodrigo-dev/zones/us-central1-a/instanceGroupManagers/gke-m8s-dev-1-default-pool-2daaf601-grp
        final String instanceGroupUrl = defaultPool.getInstanceGroupUrls().get(0);
        Pattern p = Pattern.compile(
                "https://www.googleapis.com/compute/v1/projects/([^/]+)/zones/([^/]+)/instanceGroupManagers/([^/]+)");
        Matcher m = p.matcher(instanceGroupUrl);
        if (!m.find()) {
            System.out.println("Instance group of pool " + defaultPool.getName() +
                    " doesn't conform to the right format (" + instanceGroupUrl + ")");
            return false;
        }

        final String instanceGroupProject = m.group(1);
        final String instanceGroupZone = m.group(2);
        final String instanceGroupName = m.group(3);
        InstanceGroupManager instanceGroupManager = this.gceClient.instanceGroupManagers().get(
                instanceGroupProject, instanceGroupZone, instanceGroupName).execute();
        final int currentTargetSize = instanceGroupManager.getTargetSize();

        // Instance template is like
        // https://www.googleapis.com/compute/v1/projects/rodrigo-dev/global/instanceTemplates/gke-m8s-dev-1-default-pool-ac0517f6"
        p = Pattern.compile("https://www.googleapis.com/compute/v1/projects/([^/]+)/global/instanceTemplates/([^/]+)");
        m = p.matcher(instanceGroupManager.getInstanceTemplate());
        if (!m.find()) {
            System.out.println("Instance template of instance group " + instanceGroupManager.getSelfLink() +
                    " doesn't conform to the right format (" + instanceGroupManager.getInstanceTemplate() + ")");
            return false;
        }
        final String instanceTemplateProject = m.group(1);
        final String instanceTemplateName = m.group(2);
        final InstanceTemplate instanceTemplate = this.gceClient.instanceTemplates().get(
                instanceTemplateProject, instanceTemplateName).execute();

        // Machine type is like n1-standard-1
        final MachineType machineType = this.gceClient.machineTypes().get(
                instanceGroupProject,
                instanceGroupZone,
                instanceTemplate.getProperties().getMachineType()).execute();

        final int numVmToBeScaledUp = Math.max(
            resourceToBeScaledUp.numCpu / machineType.getGuestCpus() + 1,
            resourceToBeScaledUp.numMemMB / machineType.getMemoryMb() + 1
        );

        // API reference: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters.nodePools/setSize
        final String poolName = String.format("projects/%s/locations/%s/clusters/%s/nodePools/%s",
                projectId, location, cluster, defaultPool.getName());
        final int newTargetSize = currentTargetSize + numVmToBeScaledUp;
        System.out.println("Resize node pool " + poolName + " from " + currentTargetSize + " to " + newTargetSize +
                " (Machine type " + machineType.getGuestCpus() + "cpus, " + machineType.getMemoryMb() + "mem MBs)");
        SetNodePoolSizeRequest setNodePoolSizeRequest = new SetNodePoolSizeRequest().setNodeCount(newTargetSize);
        Container.Projects.Locations.Clusters.NodePools.SetSize setSizeRequest =
                this.gkeClient.projects().locations().clusters().nodePools().setSize(poolName, setNodePoolSizeRequest);

        // This resize request will fail if the previous resize hasn't finished reconciliation.
        setSizeRequest.execute();
        return true;
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
