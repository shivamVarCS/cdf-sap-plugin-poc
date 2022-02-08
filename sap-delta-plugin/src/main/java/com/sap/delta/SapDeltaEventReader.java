package com.sap.delta;

import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.StopContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SapDeltaEventReader implements EventReader {

  private static final Logger LOG = LoggerFactory.getLogger(SapDeltaEventReader.class);

  final EventReaderDefinition eventReaderDefinition;
  final DeltaSourceContext deltaSourceContext;
  private final ExecutorService executorService;

  private final StorageWatcher storageWatcher;

  public SapDeltaEventReader(EventReaderDefinition eventReaderDefinition,
                             DeltaSourceContext deltaSourceContext, EventEmitter eventEmitter) {
    this.eventReaderDefinition = eventReaderDefinition;
    this.deltaSourceContext = deltaSourceContext;
    this.eventEmitter = eventEmitter;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.storageWatcher = new StorageWatcher();
  }

  final EventEmitter eventEmitter;

  @Override
  public void start(Offset offset) {
    LOG.info("inside start");
    LOG.info(new Gson().toJson(offset.get()));
    LOG.info(new Gson().toJson(eventReaderDefinition.getTables()));


    eventReaderDefinition.getTables().forEach(sourceTable -> {
      LOG.info("Table: {}", sourceTable.getTable());
      LOG.info("Database: {}", sourceTable.getDatabase());
      LOG.info("Schema: {}", sourceTable.getSchema());
      LOG.info("Column Size: {}",sourceTable.getColumns().size());
    });

//    String dbName = eventReaderDefinition.getTables().stream().findFirst().get().getDatabase();
//    String schName = eventReaderDefinition.getTables().stream().findFirst().get().getSchema();
//    String tabName = eventReaderDefinition.getTables().stream().findFirst().get().getTable();
//
//    List<Schema.Field> fieldList = new ArrayList<>();
//    eventReaderDefinition.getTables().stream().findFirst().get().getColumns().
//
//    Schema.Field field = Schema.Field.of("Col1", false ?
//      Schema.nullableOf(Schema.of(Schema.Type.STRING)) : Schema.of(Schema.Type.STRING));
//    Schema schema = Schema.recordOf("outputSchema", Collections.singletonList(field));
//
//    StructuredRecord value = StructuredRecord.builder(schema).set("Col1","Anup").build();
//
//    executorService.submit(() -> {
//      try {
//        int i = 0;
//        Offset offset1 = new Offset(Collections.emptyMap());
//        do {
//          if(storageWatcher.pollFiles(offset1)) {
//            offset1 = storageWatcher.getNewOffset();
//            DMLEvent.Builder dmlBuilder = DMLEvent.builder()
//              .setOffset(storageWatcher.getNewOffset())
//              .setOperationType(DMLOperation.Type.INSERT)
//              .setDatabaseName(dbName)
//              .setSchemaName(schName)
//              .setTableName(tabName)
//              .setRow(value)
//              .setSnapshot(false)
//              .setTransactionId(null)
//              .setIngestTimestamp(0L);
//
//            LOG.info("Emitting DML event from the Plugin, count: {}", i);
//            eventEmitter.emit(dmlBuilder.build());
//          }
//          TimeUnit.SECONDS.sleep(2);
//          i++;
//        } while (true);
//      } catch (InterruptedException e) {
//        LOG.error("Failed to emmit event", e);
//      }
//    });
  }

  @Override
  public void stop() throws InterruptedException {
    LOG.info("inside stop");
  }

  @Override
  public void stop(StopContext context) throws InterruptedException {
    LOG.info("inside stop with 1 parameter");
    executorService.shutdownNow();
    if (!executorService.awaitTermination(2, TimeUnit.MINUTES)) {
      LOG.warn("Unable to cleanly shutdown reader within the timeout.");
    }
  }
}
