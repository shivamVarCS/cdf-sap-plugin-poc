package com.sap.delta.metadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class SapDeltaTable {

  private String databaseName;
  private String tableName;
  private String schema;
  private List<SapDeltaSourceColumn> columns;

  public SapDeltaTable() {
    databaseName = "sap_delta";
    tableName = "sap_delta_source_table";
    schema = "sap_schema";
    columns = Arrays.asList(new SapDeltaSourceColumn(tableName,1),new SapDeltaSourceColumn(tableName,2));
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getSchema() {
    return schema;
  }

  public List<SapDeltaSourceColumn> getColumns() {
    return columns;
  }

  public List<String> primaryKeyName(){
    return columns.stream()
      .filter(SapDeltaSourceColumn::isKey)
      .map(SapDeltaSourceColumn::getName)
      .collect(Collectors.toList());
  }
}
