package org.keedio.kds.logs;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Created by ivanrozas on 27/3/17.
 */
public class LogMetadataService implements Serializable {

  private String datetime;
  private String timezone;
  private String hostname;
  private String level;
  private LogPayLoad log;

  public LogMetadataService(@JsonProperty("datetime") String datetime, @JsonProperty("timezone") String timezone,
                            @JsonProperty("hostname") String hostname, @JsonProperty("level") String level,
                            @JsonProperty("log") LogPayLoad log) {

    this.datetime = datetime;
    this.timezone = timezone;
    this.hostname = hostname;
    this.level = level;
    this.log = log;
  }

  public String getDatetime() {
    return datetime;
  }

  public void setDatetime(String datetime) {
    this.datetime = datetime;
  }

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public LogPayLoad getLog() {
    return log;
  }

  public void setLog(LogPayLoad log) {
    this.log = log;
  }
}
