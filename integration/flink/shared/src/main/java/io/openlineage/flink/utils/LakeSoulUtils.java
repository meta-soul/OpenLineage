/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.DefaultCatalogTable;
import org.apache.flink.table.types.logical.RowType;

public class LakeSoulUtils {

  private static final DBManager dbManager = new DBManager();

  public static OpenLineage.SchemaDatasetFacet getSchema(
      OpenLineageContext context, TableId table) {
    TableInfo tableInfo = dbManager.getTableInfoByNameAndNamespace(table.table(), table.schema());
    CatalogBaseTable catalogBaseTable = FlinkUtil.toFlinkCatalog(tableInfo);

    List<OpenLineage.SchemaDatasetFacetFields> fields =
        ((DefaultCatalogTable) catalogBaseTable)
            .getUnresolvedSchema().getColumns().stream()
                .map(
                    field ->
                        context
                            .getOpenLineage()
                            .newSchemaDatasetFacetFieldsBuilder()
                            .name(((Schema.UnresolvedPhysicalColumn) field).getName())
                            .type(
                                ((Schema.UnresolvedPhysicalColumn) field).getDataType().toString())
                            .build())
                .collect(Collectors.toList());
    return context.getOpenLineage().newSchemaDatasetFacet(fields);
  }

  public static String getNamespace(TableId table) {
    TableInfo tableInfo = dbManager.getTableInfoByNameAndNamespace(table.table(), table.schema());
    return tableInfo.getDomain();
  }

  public static OpenLineage.SchemaDatasetFacet getSchema(
      OpenLineageContext context, RowType rowType) {
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
