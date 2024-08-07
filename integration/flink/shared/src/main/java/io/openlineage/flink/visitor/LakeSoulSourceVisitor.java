/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.LakeSoulUtils;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.lakesoul.source.LakeSoulSource;
import org.apache.flink.lakesoul.table.LakeSoulTableSource;
import org.apache.flink.lakesoul.types.TableId;

@Slf4j
public class LakeSoulSourceVisitor extends Visitor<OpenLineage.InputDataset> {
  public LakeSoulSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object source) {
    log.info("--------openlineage-lakesoul source isdefined");
    return source instanceof LakeSoulSource || source instanceof LakeSoulTableSource;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object source) {
    TableId tableId;
    if (source instanceof LakeSoulSource) {
      LakeSoulSource lakeSoulSource = (LakeSoulSource) source;
      tableId = lakeSoulSource.getTableId();

    } else if (source instanceof LakeSoulTableSource) {
      LakeSoulTableSource lakeSoulSource = (LakeSoulTableSource) source;
      tableId = lakeSoulSource.getTableId();
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported LakeSoul Source type %s", source.getClass().getCanonicalName()));
    }
    log.info("--------openlineage-lakesoul apply source tableid:{}", tableId);

    OpenLineage openLineage = context.getOpenLineage();
    return Collections.singletonList(
        openLineage
            .newInputDatasetBuilder()
            .name(tableId.identifier())
            .namespace(LakeSoulUtils.getNamespace(tableId))
            .facets(
                openLineage
                    .newDatasetFacetsBuilder()
                    .schema(LakeSoulUtils.getSchema(context, tableId))
                    .build())
            .build());
  }
}
