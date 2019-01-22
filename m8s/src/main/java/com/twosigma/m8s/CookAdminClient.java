package com.twosigma.m8s;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

public class CookAdminClient implements CookAdminClientInterface {
    private String endpoint;

    public CookAdminClient(String endpoint) {
        this.endpoint = endpoint;
    }

    public List<CookJobInstance> getCookQueue() throws Exception {
        HttpURLConnection connection = null;

        try {
            //Create connection
            URL url = new URL(this.endpoint);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Language", "en-US");
            connection.setUseCaches(false);
            connection.setDoOutput(true);

            //Get Response
            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
            rd.close();
            return CookJobInstance.parseCookQueue(response.toString());
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
