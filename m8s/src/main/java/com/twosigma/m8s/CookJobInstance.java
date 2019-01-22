package com.twosigma.m8s;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class CookJobInstance {
    private String taskId;
    private String status;

    public CookJobInstance() {
    }

    public String getTaskId() {
        return this.taskId;
    }

    public String getStatus() {
        return this.status;
    }

    private static List<CookJobInstance> parseInstanceArray(JSONArray instances) {
        List<CookJobInstance> jobInstances = new ArrayList<>();
        for (int i = 0; i < instances.length(); i++) {
            JSONObject instance = instances.getJSONObject(i);
            CookJobInstance jobInstance = new CookJobInstance();
            jobInstance.taskId = instance.getString("instance/task-id");
            jobInstance.status = instance.getString("instance/status");
            jobInstances.add(jobInstance);
        }
        return jobInstances;
    }

    private static List<CookJobInstance> parseJobArray(JSONArray jobs) {
        List<CookJobInstance> jobInstances = new ArrayList<>();
        for (int i = 0; i < jobs.length(); i++) {
            JSONObject job = jobs.getJSONObject(i);
            JSONArray instances = job.getJSONArray("job/instance");
            jobInstances.addAll(CookJobInstance.parseInstanceArray(instances));
        }
        return jobInstances;
    }

    public static List<CookJobInstance> parseCookQueue(String jsonString) {
        List<CookJobInstance> jobInstances = new ArrayList<>();
        JSONObject obj = new JSONObject(jsonString);
        JSONArray ondemandJobs = obj.getJSONArray("ondemand");
        jobInstances.addAll(CookJobInstance.parseJobArray(ondemandJobs));

        JSONArray spotJobs = obj.getJSONArray("spot");
        jobInstances.addAll(CookJobInstance.parseJobArray(spotJobs));

        return jobInstances;
    }
}
