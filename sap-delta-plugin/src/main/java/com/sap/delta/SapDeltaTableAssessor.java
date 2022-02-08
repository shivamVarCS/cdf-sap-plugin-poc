package com.sap.delta;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.Assessment;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SapDeltaTableAssessor implements TableAssessor<TableDetail> {

  private static final Logger LOG = LoggerFactory.getLogger(SapDeltaTableAssessor.class);

  @Override
  public TableAssessment assess(TableDetail tableDetail) {
    LOG.info("inside the assess with single parameter");
    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    for (ColumnDetail columnDetail : tableDetail.getColumns()) {
      ColumnAssessment assessment = ColumnAssessment.builder(columnDetail.getName(), columnDetail.getType().getName())
        .setSupport(ColumnSupport.YES)
        .build();

      columnAssessments.add(assessment);
    }

    return new TableAssessment(columnAssessments, tableDetail.getFeatures());
  }

//  @Override
//  public Assessment assess() {
//    LOG.info("inside the assess");
//    return null;
//  }

  @Override
  public void close() throws IOException {
    LOG.info("inside the SapDeltaTableAssessor.close");
  }
}
