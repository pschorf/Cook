package com.twosigma.m8s;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.util.Config;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by rodrigo on 1/15/19.
 */
public class ApiClientBuilder {

    private ApiClientBuilder() {}

    public static ApiClient build(String filename) throws IOException {
        ApiClient apiClient = Config.fromConfig(filename);
        // Disable timeout for watches
        apiClient.getHttpClient().setReadTimeout(0, TimeUnit.MILLISECONDS);
        return apiClient;
    }

}
