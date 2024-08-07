/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.flink.streaming.api.transformations;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.LakeSoulUtils;
import io.openlineage.flink.visitor.Visitor;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.bucket.BucketsBuilder;
import org.apache.flink.lakesoul.sink.bucket.DefaultOneTableBulkFormatBuilder;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;

@Slf4j
public class LakeSoulMutilTableSinkVisitor extends Visitor<OpenLineage.OutputDataset> {
  public LakeSoulMutilTableSinkVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object sink) {
    log.info("--------openlineage-lakesoul sink isdefined");
    if (sink instanceof SinkV1Adapter.PlainSinkAdapter) {
      return ((SinkV1Adapter.PlainSinkAdapter) sink).getSink() instanceof LakeSoulMultiTablesSink;
    }
    return sink instanceof LakeSoulMultiTablesSink
        && isOneTableSink((LakeSoulMultiTablesSink) sink);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object sink) {
    TableSchemaIdentity tableSchemaIdentity = getTableIdentity(sink);
    log.info(
        "--------openlineage-lakesoul apply sink,tablename{}", tableSchemaIdentity.tableId.table());
    OpenLineage openLineage = context.getOpenLineage();
    return Collections.singletonList(
        openLineage
            .newOutputDatasetBuilder()
            .name(tableSchemaIdentity.tableId.identifier())
            .namespace(LakeSoulUtils.getNamespace(tableSchemaIdentity.tableId))
            .facets(
                openLineage
                    .newDatasetFacetsBuilder()
                    .schema(LakeSoulUtils.getSchema(context, tableSchemaIdentity.rowType))
                    .build())
            .build());
  }

  private boolean isOneTableSink(LakeSoulMultiTablesSink sink) {
    BucketsBuilder bucketsBuilder = sink.getBucketsBuilder();
    return bucketsBuilder instanceof DefaultOneTableBulkFormatBuilder;
  }

  private TableSchemaIdentity getTableIdentity(Object sink) {
    Object lakeObject = null;
    if (sink instanceof SinkV1Adapter.PlainSinkAdapter) {
      lakeObject = ((SinkV1Adapter.PlainSinkAdapter) sink).getSink();
    }
    if (lakeObject instanceof LakeSoulMultiTablesSink) {
      LakeSoulMultiTablesSink lakeSoulMultiTablesSink = (LakeSoulMultiTablesSink) lakeObject;
      BucketsBuilder bucketsBuilder = lakeSoulMultiTablesSink.getBucketsBuilder();
      if (bucketsBuilder instanceof DefaultOneTableBulkFormatBuilder) {
        DefaultOneTableBulkFormatBuilder builder =
            (DefaultOneTableBulkFormatBuilder) bucketsBuilder;
        return builder.getIdentity();
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported LakeSoul sink type %s", bucketsBuilder.getClass().getCanonicalName()));
      }
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported LakeSoul sink type %s", lakeObject.getClass().getCanonicalName()));
    }
  }
}
