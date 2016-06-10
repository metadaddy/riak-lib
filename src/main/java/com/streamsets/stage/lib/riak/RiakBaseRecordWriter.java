/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.stage.lib.riak;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class RiakBaseRecordWriter implements RiakRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(RiakBaseRecordWriter.class);
  private final List<RiakFieldMappingConfig> customMappings;

  private RiakClient client;
  private String tableName;

  private Map<String, String> columnsToFields = new HashMap<>();

  public String getColumnType(String columnName) {
    return columnType.get(columnName);
  }

  // Need to preserve ordering of columns
  private Map<String, String> columnType = new LinkedHashMap<>();

  public Map<String, String> getColumnType() {
    return columnType;
  }

  public RiakBaseRecordWriter(
      RiakClient client,
      String tableName,
      List<RiakFieldMappingConfig> customMappings
  ) throws StageException {
    this.client = client;
    this.tableName = tableName;
    this.customMappings = customMappings;

    createDefaultFieldMappings();
    createCustomFieldMappings();
  }

  private void createDefaultFieldMappings() throws StageException {
    String queryText = "DESCRIBE " + tableName;
    Query query = new Query.Builder(queryText).build();
    QueryResult queryResult = null;
    try {
      queryResult = client.execute(query);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Failed to get schema for table {}", tableName, e);
      throw new StageException(Errors.RIAK_01, tableName);
    }

    // Column descriptions should be "Column", "Type", "Is Null", "Primary Key", "Local Key"
    // TODO: Check with Basho that this is reliable!
    List<ColumnDescription> md = queryResult.getColumnDescriptionsCopy();
    if (!md.get(0).getName().equals("Column")) {
      LOG.error("Failed to get schema for table {}", tableName);
      throw new StageException(Errors.RIAK_01, tableName);
    }

    // Each row of this query result represents a column in the table
    for (Row row : queryResult.getRowsCopy()) {
      // Each cell of this row represents an attribute of the column
      List<Cell> cells = row.getCellsCopy();
      String columnName = cells.get(0).getVarcharAsUTF8String();
      columnsToFields.put(columnName, "/" + columnName); // Default implicit field mappings
      columnType.put(columnName, cells.get(1).getVarcharAsUTF8String());
    }
  }

  private void createCustomFieldMappings() {
    for (RiakFieldMappingConfig mapping : customMappings) {
      LOG.debug("Custom mapping field {} to column {}", mapping.field, mapping.columnName);
      if (columnsToFields.containsKey(mapping.columnName)) {
        LOG.debug("Mapping field {} to column {}", mapping.field, mapping.columnName);
        columnsToFields.put(mapping.columnName, mapping.field);
      }
    }
  }

  /**
   * Table this writer will write to.
   * @return table name
   */
  protected String getTableName() {
    return tableName;
  }

  /**
   * Riak client used for writing.
   * @return JDBC DataSource
   */
  RiakClient getClient() {
    return client;
  }

  /**
   * Riak Table to SDC Field mappings
   * @return map of the mappings
   */
  Map<String, String> getColumnsToFields() {
    return columnsToFields;
  }

  protected Cell makeCell(Field field, String type) {
    Cell cell = null;

    if (type.equalsIgnoreCase("varchar")) {
      cell = new Cell(field.getValueAsString());
    } else if (type.equalsIgnoreCase("boolean")) {
      cell = new Cell(field.getValueAsBoolean());
    } else if (type.equalsIgnoreCase("timestamp")) {
      cell = new Cell(field.getValueAsDatetime());
    } else if (type.equalsIgnoreCase("sint64")) {
      cell = new Cell(field.getValueAsLong());
    } else if (type.equalsIgnoreCase("double")) {
      cell = new Cell(field.getValueAsDouble());
    }

    return cell;
  }
}
