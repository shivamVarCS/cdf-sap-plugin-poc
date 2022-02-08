package com.sap.delta.metadata;

import java.sql.JDBCType;

public class SapDeltaSourceColumn {
  private String name;
  private int type;
  private boolean nullable;

  public SapDeltaSourceColumn(String tableName, int index) {
    name = tableName.concat("_").concat("Col").concat(String.valueOf(index));
    type = 12;
    nullable = false;
  }

  public String getName() {
    return name;
  }

  public JDBCType getType() {
    return JDBCType.valueOf(type);
  }

  public boolean isNullable() {
    return nullable;
  }

  public boolean isKey(){
    return  !nullable;
  }
}
