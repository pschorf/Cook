package com.twosigma.m8s;

import java.util.List;

public interface CookAdminClientInterface {
    List<CookJob> getCookQueue() throws Exception;
}
