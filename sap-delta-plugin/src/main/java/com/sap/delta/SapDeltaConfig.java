package com.sap.delta;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

public class SapDeltaConfig  extends PluginConfig {

  @Description("Hostname or IP address of the SqlServer to read from.")
  private String host;

  @Description("Port to use to connect to the SqlServer.")
  private int port;

  @Description("Username to use to connect to the SqlServer.")
  private String user;

  @Macro
  @Description("Password to use to connect to the SqlServer.")
  private String password;

  @Description("Database to replicate data from.")
  private String database;

  @Nullable
  @Description("Timezone of the SqlServer. This is used when converting dates into timestamps.")
  private String serverTimezone;

  @Nullable
  @Description("Whether to replicate existing data from the source database. By default, pipeline will replicate " +
    "the existing data from source tables. If set to false, any existing data in the source " +
    "tables will be ignored and only changes happening after the pipeline started will be replicated.")
  private Boolean replicateExistingData;

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public String getDatabase() {
    return database;
  }

  @Nullable
  public String getServerTimezone() {
    return serverTimezone;
  }

  @Nullable
  public Boolean getReplicateExistingData() {
    return replicateExistingData;
  }

  public SapDeltaConfig(String host, int port, String user, String password, String database,
                        @Nullable String serverTimezone,
                        @Nullable Boolean replicateExistingData) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.database = database;
    this.serverTimezone = serverTimezone;
    this.replicateExistingData = replicateExistingData;
  }
}
