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
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.Row;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class RiakGenericRecordWriter extends RiakBaseRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(RiakGenericRecordWriter.class);

  /**
   * Class constructor
   * @param client Riak client object
   * @param tableName the name of the table to write to
   * @param customMappings any custom mappings the user provided
   * @throws StageException
   */
  public RiakGenericRecordWriter(
      RiakClient client,
      String tableName,
      List<RiakFieldMappingConfig> customMappings
  ) throws StageException {
    super(client, tableName, customMappings);
  }

  /** {@inheritDoc} */
  @Override
  public List<OnRecordErrorException> writeBatch(Collection<Record> batch) throws StageException {
    List<OnRecordErrorException> errorRecords = new LinkedList<>();

    try {
      List<Row> rows = new ArrayList<Row>();

      for (Record record : batch) {
        List<Cell> cells = new ArrayList<>();
        // Iterate through table columns
        for (String columnName : getColumnType().keySet()) {
          String fieldPath = getColumnsToFields().get(columnName);
          Cell cell = null;
          if (record.has(fieldPath)) {
            Field field = record.get(getColumnsToFields().get(columnName));
            cell = makeCell(field, getColumnType(columnName));
          }
          cells.add(cell);
        }
        Store storeCmd = new Store.Builder(getTableName()).withRow(new Row(cells)).build();
        getClient().execute(storeCmd);
      }
    } catch (Exception e) {
      LOG.error("Error writing records", e);
      throw new StageException(Errors.RIAK_14, e);
    }

    return errorRecords;
  }
}
