package com.twosigma.m8s;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.InstanceGroupManager;
import com.google.api.services.compute.model.InstanceGroupManagersDeleteInstancesRequest;
import com.google.api.services.compute.model.InstanceTemplate;
import com.google.api.services.compute.model.MachineType;
import io.kubernetes.client.ApiClient;

import com.google.api.services.container.model.ListNodePoolsResponse;
import com.google.api.services.container.model.NodePool;
import com.google.api.services.container.model.SetNodePoolSizeRequest;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.services.container.Container;
import com.google.api.services.container.ContainerScopes;
import io.kubernetes.client.ApiException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class AutoScaler {
    private static final String APPLICATION_NAME = "AutoScaler";
    private static final long POLL_PERIOD_SECONDS = 10;
    private static final long MIN_TIME_BETWEEN_SCALE_UP_SECONDS = 60;
    private static final int MIN_NODE_IDLE_TIME_BEFORE_DELETION = 5;

    private final String projectId;
    private final String location;
    private final String cluster;
    private final ApiClient k8sClient;
    private final Container gkeClient;
    private final Compute gceClient;
    private final CookAdminClientInterface cookAdminClient;
    private final M8s m8sClient;
    private long lastScaleUpTimeMilliseconds;
    private Map<String, Integer> idleNodeToIdleTime;

    private static HttpTransport httpTransport;
    private static DataStoreFactory dataStoreFactory;
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
        public double numCpu;
        public double numMemMB;
        public Resource() {
            this.numCpu = 0;
            this.numMemMB = 0;
        }
    }

    private class IGMInfo {
        public String project;
        public String zone;
        public String name;
        public IGMInfo() {
        }
    }

    public AutoScaler(
            String projectId,
            String location,
            String cluster,
            ApiClient k8sClient,
            Container gkeClient,
            Compute gceClient,
            CookAdminClientInterface cookAdminClient,
            M8s m8sClient) {
        this.projectId = projectId;
        this.location = location;
        this.cluster = cluster;
        this.k8sClient = k8sClient;
        this.gkeClient = gkeClient;
        this.cookAdminClient = cookAdminClient;
        this.gceClient = gceClient;
        this.m8sClient = m8sClient;
        this.lastScaleUpTimeMilliseconds = 0;
        this.idleNodeToIdleTime = new HashMap<>();
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

            // Check if scaled up is needed
            Resource resourceToBeScaledUp = null;
            try {
                resourceToBeScaledUp = this.cookQueueBusy();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Fail to query cook queue");
                continue;
            }

            if (resourceToBeScaledUp == null ||
                    (resourceToBeScaledUp.numCpu == 0.0 && resourceToBeScaledUp.numMemMB == 0.0)) {
                // Scale down logic
                try {
                    System.out.println("No scale up needed. Considered scaling down.");
                    this.scaleDown();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Fail to scale down nodes");
                }
            } else {
                // Scale up logic
                try {
                    System.out.println("Need to scale up " + resourceToBeScaledUp.numCpu + " cpus "
                            + resourceToBeScaledUp.numMemMB + " mem MBs");
                    if (this.scaleUpNodePool(resourceToBeScaledUp)) {
                        this.lastScaleUpTimeMilliseconds = System.currentTimeMillis();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Fail to scale up nodes");
                }
            }
        }
    }

    public void scaleDown() throws ApiException, IOException {
        final Set<String> idleNodes = this.m8sClient.getIdleNodes();
        Map<String, Integer> newIdleNodeToIdleTime = new HashMap<>();
        for (String node: idleNodes) {
            newIdleNodeToIdleTime.put(node, 1);
        }

        Set<String> idleNodesToBeDeleted = new HashSet<>();
        for (String node : newIdleNodeToIdleTime.keySet()) {
            if (!this.idleNodeToIdleTime.containsKey(node)) {
                continue;
            }
            int numIdleTimes = newIdleNodeToIdleTime.get(node) + this.idleNodeToIdleTime.get(node);
            newIdleNodeToIdleTime.put(
                    node,
                    newIdleNodeToIdleTime.get(node) + this.idleNodeToIdleTime.get(node));
            if (numIdleTimes > AutoScaler.MIN_NODE_IDLE_TIME_BEFORE_DELETION) {
                idleNodesToBeDeleted.add(node);
            }
        }

        try {
            this.scaleDownNodes(idleNodesToBeDeleted);
            for (String node : idleNodesToBeDeleted) {
                newIdleNodeToIdleTime.remove(node);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to scale down nodes");
        }
        this.idleNodeToIdleTime = newIdleNodeToIdleTime;
    }

    public void scaleDownNodes(Set<String> nodesToBeScaledDown) throws IOException {
        final NodePool defaultPool = this.getDefaultPool();
        final IGMInfo igmInfo = this.getIGMInfo(defaultPool);

        List<String> nodeLists = new ArrayList<>();
        nodeLists.addAll(nodesToBeScaledDown);
        InstanceGroupManagersDeleteInstancesRequest request = new InstanceGroupManagersDeleteInstancesRequest();
        request.setInstances(nodeLists);

        Compute.InstanceGroupManagers managers = this.gceClient.instanceGroupManagers();
        Compute.InstanceGroupManagers.DeleteInstances deleteInstance = managers.deleteInstances(
                igmInfo.project, igmInfo.zone, igmInfo.name, request);
        deleteInstance.execute();
    }

    public Resource cookQueueBusy() throws Exception {
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

    public NodePool getDefaultPool() throws IOException {
        String parent = String.format("projects/%s/locations/%s/clusters/%s", projectId, location, cluster);
        // API reference: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters.nodePools/list
        Container.Projects.Locations.Clusters.NodePools.List listNodePoolsRequest =
                this.gkeClient.projects().locations().clusters().nodePools().list(parent);
        ListNodePoolsResponse listNodePoolsResponse = listNodePoolsRequest.execute();
        List<NodePool> pools = listNodePoolsResponse.getNodePools();

        // TODO: We only consider 1 node pool for now
        return pools.get(0);
    }

    public IGMInfo getIGMInfo(NodePool pool) throws IOException {
        // Machine group url is like:
        // https://www.googleapis.com/compute/v1/projects/rodrigo-dev/zones/us-central1-a/instanceGroupManagers/gke-m8s-dev-1-default-pool-2daaf601-grp
        final String instanceGroupUrl = pool.getInstanceGroupUrls().get(0);
        Pattern p = Pattern.compile(
                "https://www.googleapis.com/compute/v1/projects/([^/]+)/zones/([^/]+)/instanceGroupManagers/([^/]+)");
        Matcher m = p.matcher(instanceGroupUrl);
        if (!m.find()) {
            System.out.println("Instance group of pool " + pool.getName() +
                    " doesn't conform to the right format (" + instanceGroupUrl + ")");
            return null;
        }

        IGMInfo info = new IGMInfo();
        info.project = m.group(1);
        info.zone = m.group(2);
        info.name = m.group(3);
        return info;
    }

    // Super simple, get current node pool size and increment by 1
    public boolean scaleUpNodePool(Resource resourceToBeScaledUp) throws IOException {
        // TODO: We only consider 1 node pool for now
        final NodePool defaultPool = this.getDefaultPool();
        final IGMInfo igmInfo = this.getIGMInfo(defaultPool);

        InstanceGroupManager instanceGroupManager = this.gceClient.instanceGroupManagers().get(
                igmInfo.project, igmInfo.zone, igmInfo.name).execute();
        final int currentTargetSize = instanceGroupManager.getTargetSize();

        // Instance template is like
        // https://www.googleapis.com/compute/v1/projects/rodrigo-dev/global/instanceTemplates/gke-m8s-dev-1-default-pool-ac0517f6"
        Pattern p = Pattern.compile("https://www.googleapis.com/compute/v1/projects/([^/]+)/global/instanceTemplates/([^/]+)");
        Matcher m = p.matcher(instanceGroupManager.getInstanceTemplate());
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
                igmInfo.project,
                igmInfo.zone,
                instanceTemplate.getProperties().getMachineType()).execute();

        final int numVmToBeScaledUp = Math.max(
            Math.toIntExact(Math.round(resourceToBeScaledUp.numCpu / machineType.getGuestCpus())) + 1,
            Math.toIntExact(Math.round(resourceToBeScaledUp.numMemMB / machineType.getMemoryMb())) + 1
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

    public int getNodePoolTargetSize() throws IOException {
        final NodePool defaultPool = this.getDefaultPool();
        final IGMInfo igmInfo = this.getIGMInfo(defaultPool);

        InstanceGroupManager instanceGroupManager = this.gceClient.instanceGroupManagers().get(
                igmInfo.project, igmInfo.zone, igmInfo.name).execute();
        return instanceGroupManager.getTargetSize();
    }

    public void scaleNodePool(int targetSize) throws IOException {
        // TODO: We only consider 1 node pool for now
        final NodePool defaultPool = this.getDefaultPool();

        // API reference: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters.nodePools/setSize
        final String poolName = String.format("projects/%s/locations/%s/clusters/%s/nodePools/%s",
                projectId, location, cluster, defaultPool.getName());
        SetNodePoolSizeRequest setNodePoolSizeRequest = new SetNodePoolSizeRequest().setNodeCount(targetSize);
        Container.Projects.Locations.Clusters.NodePools.SetSize setSizeRequest =
                this.gkeClient.projects().locations().clusters().nodePools().setSize(poolName, setNodePoolSizeRequest);

        // This resize request will fail if the previous resize hasn't finished reconciliation.
        setSizeRequest.execute();
    }

    public static GoogleCredential getGoogleCredential(String clientSecretFilename) throws IOException {
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
    public static Container buildGkeClient(String clientSecretFilename) throws Exception {
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
    public static Compute buildGceClient(String clientSecretFilename) throws Exception {
        GoogleCredential credential = AutoScaler.getGoogleCredential(clientSecretFilename);

        // Create compute engine object for listing instances
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        Compute gceClient = new Compute.Builder(
                httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME).build();
        return gceClient;
    }

    public static void main(String[] args) throws Exception {
        String cookUrl = System.getenv("COOK_URL");
        checkNotNull(cookUrl, "COOK_URL environment variable cannot be undefined");

        String k8sConnConfigPath = System.getenv("K8S_CONN_CONFIG_PATH");
        checkNotNull(k8sConnConfigPath, "K8S_CONN_CONFIG_PATH environment variable cannot be undefined");

        String clientSecretPath = System.getenv("CLIENT_SECRET_PATH");

        CookAdminClientInterface cookAdminClient = new CookAdminClient(cookUrl);
        ApiClient k8sClient = ApiClientBuilder.build(k8sConnConfigPath);
        M8s m8sClient = new M8s(k8sClient);
        Container gkeClient = buildGkeClient(clientSecretPath);
        Compute gceClient = buildGceClient(clientSecretPath);
        AutoScaler scaler = new AutoScaler(
                "rodrigo-dev",
                "us-central1-a",
                "m8s-dev-1",
                k8sClient,
                gkeClient,
                gceClient,
                cookAdminClient,
                m8sClient);
        scaler.run();
    }
}
