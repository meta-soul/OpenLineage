/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.LakeSoulUtils;
import lombok.NonNull;
import org.apache.flink.lakesoul.source.LakeSoulSource;
import org.apache.flink.lakesoul.table.LakeSoulTableSource;
import org.apache.flink.lakesoul.types.TableId;

import java.util.Collections;
import java.util.List;

public class LakeSoulSourceVisitor extends Visitor<OpenLineage.InputDataset> {
    public LakeSoulSourceVisitor(@NonNull OpenLineageContext context) {
        super(context);
    }

    @Override public boolean isDefinedAt(Object source) {
        return source instanceof LakeSoulSource
                || source instanceof LakeSoulTableSource;
    }

    @Override public List<OpenLineage.InputDataset> apply(Object source) {
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
        OpenLineage openLineage = context.getOpenLineage();
        return Collections.singletonList(openLineage
                .newInputDatasetBuilder()
                .name(tableId.table())
                .namespace(tableId.schema())
                .facets(
                        openLineage
                                .newDatasetFacetsBuilder()
                                .schema(LakeSoulUtils.getSchema(context, tableId))
                                .build())
                .build());
    }
}
