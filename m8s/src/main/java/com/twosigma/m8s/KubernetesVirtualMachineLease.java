package com.twosigma.m8s;

import com.netflix.fenzo.VirtualMachineLease;

import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Offer;

import java.lang.Double;
import java.lang.String;
import java.util.List;
import java.util.Map;

/**
 * Created by rodrigo on 1/15/19.
 */
public class KubernetesVirtualMachineLease implements VirtualMachineLease {

    public KubernetesVirtualMachineLease() {}

    public double cpuCores() {
        return 0.0;
    }

    public double diskMB() {
        return 0.0;
    }

    public Map<String, Attribute> getAttributeMap() {
        return null;
    }

    public String getId() {
        return null;
    }

    public Offer getOffer() {
        return null;
    }

    public long getOfferedTime() {
        return 0;
    }

    public Double getScalarValue(String name) {
        return null;
    }

    public Map<String, Double> getScalarValues() {
        return null;
    }

    public String getVMID() {
        return null;
    }

    public String hostname() {
        return null;
    }

    public double memoryMB() {
        return 0.0;
    }

    public double networkMbps() {
        return 0.0;
    }

    public List<VirtualMachineLease.Range> portRanges() {
        return null;
    }
}
