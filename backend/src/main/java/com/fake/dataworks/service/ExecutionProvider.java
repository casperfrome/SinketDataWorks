package com.fake.dataworks.service;

import com.fake.dataworks.domain.StudioObject;
import java.util.Map;

/** Adapter boundary. Implementations must never use the metadata connection for user code. */
public interface ExecutionProvider {
    Map<String,Object> start(StudioObject snapshot,String mode,boolean simulateFailure);
    Map<String,Object> stop(String runId);
}
