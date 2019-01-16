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
                        for (String key : requests.keySet()) {
                            if (key.equals("cpu")) {
                                cpuSeen = true;
                            }
                            renderedByContainer.put(key, requests.get(key));
                        }
                    }
                    // Override with limits if they exists.
                    Map<String, Quantity> limits = v1Container.getResources().getLimits();
                    if (limits != null) {
                        for (String key : limits.keySet()) {
                            if (key.equals("cpu")) {
                                cpuSeen = true;
                            }
                            renderedByContainer.put(key, limits.get(key));
                        }
                    }
                    if (!cpuSeen) {
                        renderedByContainer.put("cpu", Quantity.fromString("100m"));
                    }
                }
                Map<String, Quantity> existing = used.get(node);
                used.put(node, this.sumMaps(existing, renderedByContainer));
            }
        }
        return used;
    }

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

    public void startPod(String username, double cpus, int memoryMb, String image, String command, String uuid) throws ApiException {
        String namespace = "default";
        Map<String, Quantity> requests = new HashMap<>();
        requests.put("cpu", Quantity.fromString(Double.toString(cpus)));
        requests.put("memory", Quantity.fromString(memoryMb + "Mi"));

        V1Pod pod = new V1PodBuilder()
                .withApiVersion("v1")
                .withKind("Pod")
                .withMetadata(new V1ObjectMetaBuilder()
                        .withName(uuid)
                        .build())
                .withSpec(new V1PodSpecBuilder()
                        .withVolumes(new V1VolumeBuilder()
                                .withName("kerberosticket")
                                .withSecret(new V1SecretVolumeSourceBuilder()
                                        .withSecretName(SECRET_PREFIX + username)
                                        .withItems(new V1KeyToPathBuilder()
                                                .withKey("ts_kerberos_ticket")
                                                .withPath("kerbticket")
                                                .build())
                                        .build())
                                .build())
                        .withContainers(new V1ContainerBuilder()
                                .withName("container-name")
                                .withVolumeMounts(new V1VolumeMountBuilder()
                                        .withName("kerberosticket")
                                        .withMountPath("/tmp")
                                        .withReadOnly(true)
                                        .build())
                                .withResources(new V1ResourceRequirementsBuilder()
                                        .withRequests(requests)
                                        .build())
                                .withImage(image)
                                //.withCommand(command)
                                .build())
                        .build())
                .build();
        this.coreV1Api.createNamespacedPod(namespace, pod, null);
    }

    public void pollPodEvents(PodEventNotifier notifier) throws ApiException {
        Call call = this.coreV1Api.listEventForAllNamespacesCall(null, null, null, null, null, null, null, null, true, null, null);
        Watch<V1Event> eventsWatch = Watch.createWatch(this.apiClient,
                call,
                new TypeToken<Watch.Response<V1Event>>() {}.getType());
        executorService.execute(new PodEventWatcher(eventsWatch, notifier));
    }

}
