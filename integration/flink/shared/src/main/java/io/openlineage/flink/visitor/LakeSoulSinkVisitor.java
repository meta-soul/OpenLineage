/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.LakeSoulUtils;
import lombok.NonNull;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.bucket.BucketsBuilder;
import org.apache.flink.lakesoul.sink.bucket.DefaultOneTableBulkFormatBuilder;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;

import java.util.Collections;
import java.util.List;

public class LakeSoulSinkVisitor extends Visitor<OpenLineage.OutputDataset> {
    public LakeSoulSinkVisitor(@NonNull OpenLineageContext context) {
        super(context);
    }

    @Override public boolean isDefinedAt(Object sink) {
        return sink instanceof LakeSoulMultiTablesSink && isOneTableSink((LakeSoulMultiTablesSink) sink);
    }

    @Override public List<OpenLineage.OutputDataset> apply(Object sink) {
        TableSchemaIdentity tableSchemaIdentity = getTableIdentity(sink);
        OpenLineage openLineage = context.getOpenLineage();
        return Collections.singletonList(openLineage
                .newOutputDatasetBuilder()
                .name(tableSchemaIdentity.tableId.table())
                .namespace(tableSchemaIdentity.tableId.schema())
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
        if (sink instanceof LakeSoulMultiTablesSink) {
            LakeSoulMultiTablesSink lakeSoulMultiTablesSink = (LakeSoulMultiTablesSink) sink;
            BucketsBuilder bucketsBuilder = lakeSoulMultiTablesSink.getBucketsBuilder();
            if (bucketsBuilder instanceof DefaultOneTableBulkFormatBuilder) {
                DefaultOneTableBulkFormatBuilder builder = (DefaultOneTableBulkFormatBuilder) bucketsBuilder;
                return builder.getIdentity();
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported LakeSoul sink type %s", bucketsBuilder.getClass().getCanonicalName()));
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported LakeSoul sink type %s", sink.getClass().getCanonicalName()));
        }
    }
}
