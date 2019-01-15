package com.twosigma.m8s;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;

import java.io.IOException;

/**
 * Created by rodrigo on 1/15/19.
 */
public class Application {

    public static void main(String[] args) throws IOException, ApiException {
        ApiClient apiClient = ApiClientBuilder.build("config/m8s-dev-1.yaml");
        M8s m8 = new M8s(apiClient);
        m8.pollPodEventsFromNamespace("default", 1000, 5);
    }

}
