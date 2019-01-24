package com.twosigma.m8s;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CookJob {
    private String uuid;
    private int numCpu = 0;
    private int numMemMB = 0;

    public CookJob() {
    }

    public String getUuid() {
        return this.uuid;
    }

    public int getNumCpu() { return this.numCpu; }

    public int getNumMemMB() { return this.numMemMB; }

    private static List<CookJob> parseJobArray(JSONArray poolJobs) {
        List<CookJob> jobs = new ArrayList<>();
        for (int i = 0; i < poolJobs.length(); i++) {
            JSONObject jobObject = poolJobs.getJSONObject(i);

            CookJob job = new CookJob();
            job.uuid = jobObject.getString("job/uuid");

            JSONArray resources = jobObject.getJSONArray("job/resource");
            for (int j = 0; j < resources.length(); ++j) {
                JSONObject resource = resources.getJSONObject(j);
                final String resourceType = resource.getString("resource/type");
                final int resourceAmount = resource.getInt("resource/amount");
                if (resourceType.equals("resource.type/cpus")) {
                    job.numCpu = resourceAmount;
                } else if (resourceType.equals("resource.type/mem")) {
                    job.numMemMB = resourceAmount;
                } else {
                    System.out.println("Get unexpected " + resourceAmount + " of resource " + resourceType);
                }
            }
            jobs.add(job);
        }
        return jobs;
    }

    public static List<CookJob> parseCookQueue(String jsonString) {
        List<CookJob> jobs = new ArrayList<>();
        JSONObject obj = new JSONObject(jsonString);
        Iterator<String> keys = obj.keys();
        while(keys.hasNext()) {
            String pool = keys.next();
            if (!(obj.get(pool) instanceof JSONArray)) {
                System.out.println("Pool " + pool + " value is not of type array");
                continue;
            }
            JSONArray poolJobs = obj.getJSONArray(pool);
            jobs.addAll(CookJob.parseJobArray(poolJobs));
        }
        return jobs;
    }
}
