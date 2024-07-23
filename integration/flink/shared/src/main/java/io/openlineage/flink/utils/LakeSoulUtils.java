/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;

public class LakeSoulUtils {

    private static final DBManager dbManager = new DBManager();

    public static OpenLineage.SchemaDatasetFacet getSchema(OpenLineageContext context, TableId table) {
        TableInfo tableInfo =
                dbManager.getTableInfoByNameAndNamespace(table.table(), table.schema());
        CatalogBaseTable catalogBaseTable = FlinkUtil.toFlinkCatalog(tableInfo);

        List<OpenLineage.SchemaDatasetFacetFields> fields =
                catalogBaseTable.getSchema().getTableColumns().stream()
                        .map(
                                field ->
                                        context
                                                .getOpenLineage()
                                                .newSchemaDatasetFacetFieldsBuilder()
                                                .name(field.getName())
                                                .type(field.getType().toString())
                                                .build())
                        .collect(Collectors.toList());
        return context.getOpenLineage().newSchemaDatasetFacet(fields);
    }

    public static OpenLineage.SchemaDatasetFacet getSchema(OpenLineageContext context, RowType rowType) {
        List<OpenLineage.SchemaDatasetFacetFields> fields =
                rowType.getFields().stream()
                        .map(
                                field ->
                                        context
                                                .getOpenLineage()
                                                .newSchemaDatasetFacetFieldsBuilder()
                                                .name(field.getName())
                                                .type(field.getType().toString())
                                                .build())
                        .collect(Collectors.toList());
        return context.getOpenLineage().newSchemaDatasetFacet(fields);
    }
}
