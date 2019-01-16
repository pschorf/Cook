package com.twosigma.m8s;

import com.google.common.reflect.TypeToken;
import com.squareup.okhttp.Call;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.*;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.util.Watch;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rodrigo on 1/15/19.
 */
public class M8s {

    private ApiClient apiClient;
    private CoreV1Api coreV1Api;

    private ExecutorService executorService = Executors.newFixedThreadPool(5);

    public M8s(ApiClient apiClient) {
        this.apiClient = apiClient;
        this.coreV1Api = new CoreV1Api(this.apiClient);
    }

    public void startPod(double cpus, int memoryMb, String image, String command, String uuid) throws ApiException {
        String namespace = "default";
        Map<String, Quantity> requests = new HashMap<String, Quantity>();
        requests.put("cpu", Quantity.fromString(Double.toString(cpus)));
        requests.put("memory", Quantity.fromString(memoryMb + "Mi"));

        V1Pod pod = new V1PodBuilder()
                .withApiVersion("v1")
                .withKind("Pod")
                .withMetadata(new V1ObjectMetaBuilder()
                        .withName(uuid)
                        .build())
                .withSpec(new V1PodSpecBuilder()
                        .withContainers(new V1ContainerBuilder()
                                .withName("container-name")
                                .withResources(new V1ResourceRequirementsBuilder()
                                        .withRequests(requests)
                                        .build())
                                .withImage(image)
                                .withCommand(command)
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
