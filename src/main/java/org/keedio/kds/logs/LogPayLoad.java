package org.keedio.kds.logs;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Created by ivanrozas on 27/3/17.
 */
public class LogPayLoad implements Serializable {

  private String thread;
  private String fqcn;
  private String payload;

  public LogPayLoad(@JsonProperty("thread") String thread, @JsonProperty("fqcn") String fqcn,
                    @JsonProperty("payload") String payload) {

    this.thread = thread;
    this.fqcn = fqcn;
    this.payload = payload;
  }

  public String getThread() {
    return thread;
  }

  public void setThread(String thread) {
    this.thread = thread;
  }

  public String getFqcn() {
    return fqcn;
  }

  public void setFqcn(String fqcn) {
    this.fqcn = fqcn;
  }

  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }
}

