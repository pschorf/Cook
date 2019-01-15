package com.twosigma.m8s;

import com.google.common.reflect.TypeToken;
import com.squareup.okhttp.Call;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Event;
import io.kubernetes.client.util.Watch;

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

    public void pollPodEventsFromNamespace(String namespace, int limit, int timeoutSeconds) throws ApiException {
        Call call = this.coreV1Api.listNamespacedEventCall(namespace,
                null, null, null, null, null, limit, null, timeoutSeconds, true, null, null);
        Watch<V1Event> eventsWatch = Watch.createWatch(this.apiClient,
                call,
                new TypeToken<Watch.Response<V1Event>>() {}.getType());
        executorService.execute(new NamespaceEventsWatcher(eventsWatch));
    }

}
