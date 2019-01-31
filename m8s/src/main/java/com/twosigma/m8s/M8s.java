package com.twosigma.m8s;

import com.google.common.reflect.TypeToken;
import com.squareup.okhttp.Call;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.*;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.util.Watch;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rodrigo on 1/15/19.
 */
public class M8s {

    private static final String SECRET_PREFIX = "kerberosticket.";

    private ApiClient apiClient;
    private CoreV1Api coreV1Api;

    private ExecutorService executorService = Executors.newFixedThreadPool(5);

    public M8s(ApiClient apiClient) {
        this.apiClient = apiClient;
        this.coreV1Api = new CoreV1Api(this.apiClient);
    }

    public void populateOrRefreshKerberosTicket(String username) throws ApiException {
        Map<String, String> data = new HashMap<>();
        data.put("ts_kerberos_ticket", UUID.randomUUID().toString());
        V1Secret secret = new V1SecretBuilder()
                .withMetadata(new V1ObjectMetaBuilder()
                        .withName(SECRET_PREFIX + username)
                        .build())
                .withStringData(data)
                .build();
        this.coreV1Api.createNamespacedSecret("default", secret, null);
    }

    private Map<String, Quantity> sumMaps(Map<String, Quantity> x, Map<String, Quantity> y) {
        Map<String, Quantity> summed = new HashMap<>();
        Set<String> seen = new HashSet<>();
        for (Map.Entry<String, Quantity> entry : x.entrySet()) {
            String key = entry.getKey();
            seen.add(key);
            BigDecimal value = entry.getValue().getNumber();
            if (y.containsKey(key)) {
                value = value.add(y.get(key).getNumber());
            }
            summed.put(key, new Quantity(value, entry.getValue().getFormat()));
        }
        for (Map.Entry<String, Quantity> entry : y.entrySet()) {
            if (!seen.contains(entry.getKey())) {
                summed.put(entry.getKey(), entry.getValue());
            }
        }
        return summed;
    }

    private Map<String, Map<String, Quantity>> collectedUsedResources() throws ApiException {
        V1PodList pods = this.coreV1Api.listPodForAllNamespaces(null, null, null, null, null, null, null, null, null);
        Map<String, Map<String, Quantity>> used = new HashMap<>();
        for (V1Pod v1Pod : pods.getItems()) {
            if(!v1Pod.getStatus().getPhase().toLowerCase().equals("running")) {
                continue;
            }
            V1PodSpec podSpec = v1Pod.getSpec();
            String node = podSpec.getNodeName();
            if (node != null) {
                if (!used.containsKey(node)) {
                    used.put(node, new HashMap<>());
                }
                Map<String, Quantity> renderedByContainer = new HashMap<>();
                for (V1Container v1Container : podSpec.getContainers()) {
                    boolean cpuSeen = false;
                    Map<String, Quantity> requests = v1Container.getResources().getRequests();
                    if (requests != null) {
                        renderedByContainer = this.sumMaps(renderedByContainer, requests);
                    }
                    // Override with limits if they exists.
                    Map<String, Quantity> limits = v1Container.getResources().getLimits();
                    if (limits != null && requests == null) {
                        renderedByContainer = this.sumMaps(renderedByContainer, limits);
                    }
                }
                Map<String, Quantity> existing = used.get(node);
                used.put(node, this.sumMaps(existing, renderedByContainer));
            }
        }
        return used;
    }

    // Reference for allocatable
    // https://github.com/kubernetes/community/blob/master/contributors/design-proposals/node/node-allocatable.md
    // Allocable != Capacity - Used
    // Allocable == Capacity - (System Reserved + Kube Reserved)
    private Map<String, Map<String, Quantity>> collectAllocatableResources() throws ApiException {
        V1NodeList nodes = this.coreV1Api.listNode(null, null, null, null, null, null, null, null, false);
        Map<String, Map<String, Quantity>> allocatable = new HashMap<>();
        for (V1Node v1Node : nodes.getItems()) {
            Map<String, Quantity> nodeAllocatable = v1Node.getStatus().getAllocatable();
            allocatable.put(v1Node.getMetadata().getName(), nodeAllocatable);
        }
        return allocatable;
    }

    /**
     * @return Map of nodes -> Map -> resourceName -> Quantity
     * @throws ApiException
     */
    public Map<String, Map<String, Quantity>> getAvailableResources() throws ApiException {
        Map<String, Map<String, Quantity>> used = this.collectedUsedResources();
        Map<String, Map<String, Quantity>> allocatable = this.collectAllocatableResources();

        Map<String, Map<String, Quantity>> available = new HashMap<>();
        for (String node : allocatable.keySet()) {
            if (used.containsKey(node)) {
                available.put(node, new HashMap<>());
                Map<String, Quantity> usedInNode = used.get(node);
                Map<String, Quantity> availableInNode = allocatable.get(node);
                for (String resourceName : availableInNode.keySet()) {
                    if (usedInNode.containsKey(resourceName)) {
                        BigDecimal usedResource = usedInNode.get(resourceName).getNumber();
                        BigDecimal availableResource = availableInNode.get(resourceName).getNumber();
                        BigDecimal free = availableResource.subtract(usedResource);
                        Quantity freeQuantity = new Quantity(free, usedInNode.get(resourceName).getFormat());
                        available.get(node).put(resourceName, freeQuantity);
                    } else {
                        available.get(node).put(resourceName, availableInNode.get(resourceName));
                    }
                }
            } else {
                available.put(node, allocatable.get(node));
            }
        }
        return available;
    }

    public List<V1Node> listNode(boolean isReady) throws ApiException {
        List<V1Node> nodes = this.coreV1Api.listNode(null, null, null, null, null, null, null, null, false).getItems();
        if (!isReady) {
            return nodes;
        }

        List<V1Node> readyNodes = new ArrayList<V1Node>();
        for (V1Node node: nodes) {
            List<V1NodeCondition> conds = node.getStatus().getConditions();
            boolean ready = true;
            for (V1NodeCondition cond : conds) {
                if (cond.getType().equals("Ready") && cond.getStatus().equals("False")) {
                    ready = false;
                    break;
                }
                if (cond.getType().equals("NetworkUnavailable") && cond.getStatus().equals("True")) {
                    ready = false;
                    break;
                }
            }
            if (ready) {
                readyNodes.add(node);
            }
        }
        return readyNodes;
    }

    // Beware, this is inherently racy. Nothing guarantee the nodes still idle or even exists after the API calls.
    public Set<String> getIdleNodes() throws ApiException {
        V1PodList pods = this.coreV1Api.listPodForAllNamespaces(null, null, null, null, null, null, null, null, null);
        Set<String> usedNodes = new HashSet<>();
        for (V1Pod v1Pod : pods.getItems()) {
            V1PodSpec podSpec = v1Pod.getSpec();
            usedNodes.add(podSpec.getNodeName());
        }

        Set<String> allNodes = new HashSet<>();
        V1NodeList nodes = this.coreV1Api.listNode(null, null, null, null, null, null, null, null, false);
        for (V1Node v1Node : nodes.getItems()) {
            allNodes.add(v1Node.getMetadata().getName());
        }

        Set<String> idleNodes = new HashSet<>();
        for (String node : allNodes) {
            if (usedNodes.contains(node)) {
                continue;
            }
            idleNodes.add(node);
        }
        return idleNodes;
    }

    public void startPod(
            String username, double cpus, int tpus, int memoryMb, String image,
            String command, Map<String, String> environmentVariables, String nodeName,
            String uuid) throws ApiException
    {
        String namespace = "default";
        Map<String, Quantity> requests = new HashMap<>();
        Map<String, Quantity> limits = new HashMap<>();

        Quantity cpusQty = Quantity.fromString(Double.toString(cpus * 1000) + "m");
        Quantity memQty = Quantity.fromString(memoryMb + "Mi");

        requests.put("cpu", cpusQty);
        requests.put("memory", memQty);
        limits.put("cpu", cpusQty);
        limits.put("memory", memQty);

        V1ObjectMetaBuilder metaBuilder = new V1ObjectMetaBuilder()
                .withName(uuid);

        if (tpus != 0 && tpus != 8) {
            throw new IllegalArgumentException("tpus must be 0 or 8");
        }

        if (tpus > 0) {
            Map<String, String> tpuAnnotations = new HashMap<>();
            tpuAnnotations.put("tf-version.cloud-tpus.google.com", "1.11");
            metaBuilder.withAnnotations(tpuAnnotations);
            limits.put("cloud-tpus.google.com/v2", Quantity.fromString(Integer.toString(tpus)));
        }

        List<V1EnvVar> envVars = new ArrayList<>();
        if (environmentVariables != null) {
            for (Map.Entry<String, String> kv : environmentVariables.entrySet()) {
                envVars.add(new V1EnvVarBuilder()
                        .withName(kv.getKey())
                        .withValue(kv.getValue())
                        .build());
            }
        }
        envVars.add(new V1EnvVarBuilder()
                .withName("KRB5CCNAME")
                .withValue("/etc/krb5tickets/ticket")
                .build());
        envVars.add(new V1EnvVarBuilder()
                .withName("KRB5_CONFIG")
                .withValue("/etc/krb5conf/krb5.conf")
                .build());

        V1Pod pod = new V1PodBuilder()
                .withApiVersion("v1")
                .withKind("Pod")
                .withMetadata(metaBuilder.build())
                .withSpec(new V1PodSpecBuilder()
                        .withRestartPolicy("Never")
                        .withNodeName(nodeName)
                        .withVolumes(
                                new V1VolumeBuilder()
                                    .withName("kerberosticket")
                                    .withSecret(new V1SecretVolumeSourceBuilder()
                                            .withSecretName(SECRET_PREFIX + username)
                                            .withItems(new V1KeyToPathBuilder()
                                                    .withKey("ts_kerberos_ticket")
                                                    .withPath("ticket")
                                                    .build())
                                            .build())
                                    .build(),
                                new V1VolumeBuilder()
                                    .withName("krb5conf")
                                    .withConfigMap(new V1ConfigMapVolumeSourceBuilder()
                                            .withName("krb5conf")
                                            .withItems(new V1KeyToPathBuilder()
                                                    .withKey("krb5conf")
                                                    .withPath("krb5.conf")
                                                    .build())
                                            .build())
                                    .build())
                        .withContainers(new V1ContainerBuilder()
                                .withName("container-name")
                                .withEnv(envVars)
                                .withVolumeMounts(
                                        new V1VolumeMountBuilder()
                                            .withName("kerberosticket")
                                            .withMountPath("/etc/krb5tickets/")
                                            .withReadOnly(false)
                                            .build(),
                                        new V1VolumeMountBuilder()
                                            .withName("krb5conf")
                                            .withMountPath("/etc/krb5conf/")
                                            .withReadOnly(true)
                                            .build())
                                .withResources(new V1ResourceRequirementsBuilder()
                                        .withRequests(requests)
                                        .withLimits(limits)
                                        .build())
                                .withImage(image)
                                .withCommand("/bin/sh")
                                .withArgs("-c", command)
                                .build())
                        .build())
                .build();

        this.coreV1Api.createNamespacedPod(namespace, pod, null);
    }

    private Watch<V1Event> createEventsWatch(String namespace) throws ApiException {
        V1EventList previousEvents = this.coreV1Api.listNamespacedEvent(namespace, null, null, null, null, null, null, null, null, null);
        Long maxResourceVersion = Long.MIN_VALUE;
        for (V1Event event : previousEvents.getItems()) {
            Long resourceVersion = Long.valueOf(event.getMetadata().getResourceVersion());
            maxResourceVersion = Math.max(maxResourceVersion, resourceVersion);
        }
        String strResourceVersion = null;
        if (maxResourceVersion != Long.MIN_VALUE) {
            strResourceVersion = maxResourceVersion.toString();
        }
        Call call = this.coreV1Api.listNamespacedEventCall(namespace, null, null, null, null, null, null, strResourceVersion, null, true, null, null);
        return Watch.createWatch(this.apiClient, call, new TypeToken<Watch.Response<V1Event>>() {}.getType());
    }

    private Watch<V1Pod> createPodsWatch(String namespace) throws ApiException {
        V1PodList allPods = this.coreV1Api.listNamespacedPod(namespace, null, null, null, null, null, null, null, null, null);
        Long maxResourceVersion = Long.MIN_VALUE;
        for (V1Pod pod : allPods.getItems()) {
            Long resourceVersion = Long.valueOf(pod.getMetadata().getResourceVersion());
            maxResourceVersion = Math.max(maxResourceVersion, resourceVersion);
        }
        String strResourceVersion = null;
        if (maxResourceVersion != Long.MIN_VALUE) {
            strResourceVersion = maxResourceVersion.toString();
        }
        Call call = this.coreV1Api.listNamespacedPodCall(namespace, null, null, null, null, null, null, strResourceVersion, null, Boolean.TRUE, null, null);
        return Watch.createWatch(this.apiClient, call, new TypeToken<Watch.Response<V1Pod>>() {}.getType());
    }

    private Watch<V1Node> createNodesWatch() throws ApiException {
        V1NodeList allNodes = this.coreV1Api.listNode(null, null, null, true, null, null, null, null, null);
        Long maxResourceVersion = Long.MIN_VALUE;
        for (V1Node node : allNodes.getItems()) {
            Long resourceVersion = Long.valueOf(node.getMetadata().getResourceVersion());
            maxResourceVersion = Math.max(maxResourceVersion, resourceVersion);
        }
        String strResourceVersion = null;
        if (maxResourceVersion != Long.MIN_VALUE) {
            strResourceVersion = maxResourceVersion.toString();
        }
        Call call = this.coreV1Api.listNodeCall(null, null, null, true, null, null, strResourceVersion, null, true, null, null);
        return Watch.createWatch(this.apiClient, call, new TypeToken<Watch.Response<V1Node>>() {}.getType());
    }

    public void pollPodEvents(String namespace, PodEventNotifier notifier) throws ApiException {
        Watch<V1Pod> podsWatch = this.createPodsWatch(namespace);
        Watch<V1Event> eventsWatch = this.createEventsWatch(namespace);
        executorService.execute(new EventsWatcher(eventsWatch, notifier));
        executorService.execute(new PodsWatcher(podsWatch, notifier));
    }

    public void pollNodeEvents(NodeEventNotifier notifier) throws ApiException {
        Watch<V1Node> nodesWatch = this.createNodesWatch();
        executorService.execute(new NodesWatcher(nodesWatch, notifier));
    }

}
