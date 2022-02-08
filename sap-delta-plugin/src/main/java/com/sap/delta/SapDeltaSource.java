package com.sap.delta;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.SourceColumn;
import io.cdap.delta.api.SourceConfigurer;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Plugin(type = DeltaSource.PLUGIN_TYPE)
@Name(SapDeltaSource.NAME)
@Description("Delta source for Sap Delta.")
public class SapDeltaSource implements DeltaSource {

  private static final Logger LOG = LoggerFactory.getLogger(SapDeltaSource.class);

  public static final String NAME = "SapDelta";
  private final SapDeltaConfig config;

  private final Gson GSON = new Gson();

  public SapDeltaSource(SapDeltaConfig config) {
    this.config = config;
    LOG.info("inside the the Constructor.");
  }

  @Override
  public void configure(SourceConfigurer sourceConfigurer) {
    LOG.info("inside the configure.");
  }

  @Override
  public void initialize(DeltaSourceContext context) throws Exception {
    LOG.info("inside the initialize");
  }

  @Override
  public EventReader createReader(EventReaderDefinition eventReaderDefinition, DeltaSourceContext deltaSourceContext,
                                  EventEmitter eventEmitter) throws Exception {
    LOG.info("inside the createReader");
    LOG.info("EventReaderDefinition Tables: {}",eventReaderDefinition.getTables().size());
    LOG.info("DeltaSourceContext Application Name: {}", deltaSourceContext.getApplicationName());
    LOG.info("Table detail: {}", new Gson().toJson(eventReaderDefinition.getTables()));
    return new SapDeltaEventReader(eventReaderDefinition, deltaSourceContext, eventEmitter);
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) throws Exception {
    LOG.info("inside the createTableRegistry");
    return new SapDeltaTableRegistry();
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) throws Exception {
    LOG.info("inside the createTableAssessor with 1 parameter");
    return null;
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer, List<SourceTable> tables)
    throws Exception {
    LOG.info("inside the createTableAssessor with 2 parameter");
    LOG.info("source table count: {}", tables.size());
    LOG.info("Table JSON: {}",new Gson().toJson(tables));
    return new SapDeltaTableAssessor();
  }
}
