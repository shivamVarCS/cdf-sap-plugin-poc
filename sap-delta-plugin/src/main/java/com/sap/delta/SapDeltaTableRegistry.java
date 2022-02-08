package com.sap.delta;

import com.google.gson.Gson;
import com.sap.delta.metadata.SapDeltaTable;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SapDeltaTableRegistry implements TableRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(SapDeltaTableRegistry.class);

  @Override
  public TableList listTables() throws IOException {
    LOG.info("inside the listTables");
    List<TableSummary> tables = new ArrayList<>();
    SapDeltaTable sapDeltaTable = new SapDeltaTable();
    tables.add(new TableSummary(sapDeltaTable.getDatabaseName(), sapDeltaTable.getTableName(),
      sapDeltaTable.getColumns().size(), sapDeltaTable.getSchema()));
    return new TableList(tables);
  }

  @Override
  public TableDetail describeTable(String database, String table) throws TableNotFoundException, IOException {
    LOG.info("inside the describeTable with 2 parameters");
    return null;
  }

  @Override
  public TableDetail describeTable(String database, String schema, String table)
    throws TableNotFoundException, IOException {
    LOG.info("inside the describeTable with 3 parameters");
    LOG.info("database: {}, Schema: {}, Table: {}", database, schema, table);

    SapDeltaTable sapDeltaTable = new SapDeltaTable();

    List<ColumnDetail> columns = new ArrayList<>();

    sapDeltaTable.getColumns().forEach(sapDeltaSourceColumn -> {
      columns.add(new ColumnDetail(sapDeltaSourceColumn.getName(), sapDeltaSourceColumn.getType(),
        sapDeltaSourceColumn.isNullable()));
    });

    LOG.info("Column Details: {}", new Gson().toJson(sapDeltaTable));

    return TableDetail.builder(database, table, schema)
      .setColumns(columns)
      .setPrimaryKey(sapDeltaTable.primaryKeyName())
      .build();
  }

  @Override
  public StandardizedTableDetail standardize(TableDetail tableDetail) {
    LOG.info("inside the standardize");

    List<Schema.Field> columnSchemas = new ArrayList<>();
    for (ColumnDetail detail : tableDetail.getColumns()) {
      Schema.Field field = Schema.Field.of(detail.getName(), detail.isNullable() ?
        Schema.nullableOf(Schema.of(Schema.Type.STRING)) : Schema.of(Schema.Type.STRING));
      columnSchemas.add(field);
    }
    Schema schema = Schema.recordOf("outputSchema", columnSchemas);
    return new StandardizedTableDetail(tableDetail.getDatabase(), tableDetail.getTable(),
      tableDetail.getPrimaryKey(), schema);
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    LOG.info("inside the SapDeltaTableRegistry.close");
  }
}
