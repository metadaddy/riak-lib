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
package com.streamsets.stage.destination.riak;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.stage.lib.riak.*;

import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This target is an example and does not actually write to any destination.
 */
public abstract class RiakTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(RiakTarget.class);

  private static final String TABLE_NAME = "tableNameTemplate";
  private static final String EL_PREFIX = "${";

  private ELEval tableNameEval = null;

  public abstract List<String> getHostnames();
  public abstract int getPort();
  public abstract String getTableNameTemplate();
  public abstract boolean getUseMultiRowInsert();
  public abstract List<RiakFieldMappingConfig> getCustomMappings();

  private RiakClient client;

  class RecordWriterLoader extends CacheLoader<String, RiakRecordWriter> {
    @Override
    public RiakRecordWriter load(String tableName) throws Exception {
      return createRecordWriter(tableName);
    }
  }

  private final LoadingCache<String, RiakRecordWriter> recordWriters = CacheBuilder.newBuilder()
          .maximumSize(500)
          .expireAfterAccess(1, TimeUnit.HOURS)
          .build(new RecordWriterLoader());

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();
    Target.Context context = getContext();

    tableNameEval = context.createELEval(TABLE_NAME);
    validateEL(tableNameEval, getTableNameTemplate(), TABLE_NAME, Errors.RIAK_20, Errors.RIAK_21, issues);

    if (issues.isEmpty()) {
      try {
        client = RiakClient.newClient(getPort(), getHostnames());
      } catch (UnknownHostException e) {
        e.printStackTrace();
        issues.add(context.createConfigIssue(
                Groups.RIAK.name(), "config", Errors.RIAK_00, e.getMessage()
        ));
      }

      if (!getTableNameTemplate().contains(EL_PREFIX)) {
        String queryText = "DESCRIBE "+getTableNameTemplate();
        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = null;
        try {
          queryResult = client.execute(query);
        } catch (Exception e) {
          e.printStackTrace();
          issues.add(getContext().createConfigIssue(
                  Groups.RIAK.name(), "config", Errors.RIAK_01, e.getMessage()
          ));
        }
        // TODO - check column mappings
        //List<ColumnDescription> md = queryResult.getColumnDescriptionsCopy();
      }
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    client.shutdown();
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public void write(Batch batch) throws StageException {
    try {
      Multimap<String, Record> partitions = partitionBatch(batch);
      Set<String> tableNames = partitions.keySet();
      for (String tableName : tableNames) {
        List<OnRecordErrorException> errors = recordWriters.getUnchecked(tableName).writeBatch(partitions.get(tableName));
        for (OnRecordErrorException error : errors) {
          handleErrorRecord(error);
        }
      }
    } catch (OnRecordErrorException error) {
      handleErrorRecord(error);
    }

    Iterator<Record> batchIterator = batch.getRecords();

    List<Row> rows = new ArrayList<>();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      try {
        List<Cell> cells = new ArrayList<>();
        cells.add(new Cell("South Atlantic"));
        cells.add(new Cell("South Carolina"));
        cells.add(Cell.newTimestamp(1234567));
        cells.add(new Cell("hot"));
        cells.add(new Cell(24.25));
        rows.add(new Row(cells));
      } catch (Exception e) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().toError(record, Errors.RIAK_01, e.toString());
            break;
          case STOP_PIPELINE:
            throw new StageException(Errors.RIAK_01, e.toString());
          default:
            throw new IllegalStateException(
                Utils.format("Unknown OnError value '{}'", getContext().getOnErrorRecord(), e)
            );
        }
      }
    }

    Store storeCmd = new Store.Builder(getTableNameTemplate()).withRows(rows).build();
    try {
      client.execute(storeCmd);
    } catch (ExecutionException e) {
      // TODO
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO
      e.printStackTrace();
    }
  }

  private Multimap<String, Record> partitionBatch(Batch batch) {
    Multimap<String, Record> partitions = ArrayListMultimap.create();

    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();

      ELVars vars = getContext().createELVars();
      RecordEL.setRecordInContext(vars, record);

      try {
        String tableName = tableNameEval.eval(vars, getTableNameTemplate(), String.class);
        partitions.put(tableName, record);
      } catch (ELEvalException e) {
        LOG.error("Failed to evaluate expression '{}' : ", getTableNameTemplate(), e.toString(), e);
        // send to error
      }
    }

    return partitions;
  }

  private void handleErrorRecord(OnRecordErrorException error) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().toError(error.getRecord(), error);
        break;
      case STOP_PIPELINE:
        throw error;
      default:
        throw new IllegalStateException(
                Utils.format("Unknown OnError value '{}'", getContext().getOnErrorRecord(), error)
        );
    }
  }

  private RiakRecordWriter createRecordWriter(String tableName) throws StageException {
    RiakRecordWriter recordWriter;

    if (!getUseMultiRowInsert()) {
      recordWriter = new RiakGenericRecordWriter(
              client,
              tableName,
              getCustomMappings()
      );
    } else {
      recordWriter = new RiakMultiRowRecordWriter(
              client,
              tableName,
              getCustomMappings()
      );
    }

    return recordWriter;
  }

  private void validateEL(
          ELEval elEval,
          String elStr,
          String config,
          ErrorCode parseError,
          ErrorCode evalError,
          List<ConfigIssue> issues
  ) {
    ELVars vars = getContext().createELVars();
    RecordEL.setRecordInContext(vars, getContext().createRecord("validateConfigs"));
    boolean parsed = false;
    try {
      getContext().parseEL(elStr);
      parsed = true;
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(Groups.RIAK.name(), config, parseError, ex.toString(), ex));
    }
    if (parsed) {
      try {
        elEval.eval(vars, elStr, String.class);
      } catch (ELEvalException ex) {
        issues.add(getContext().createConfigIssue(Groups.RIAK.name(), config, evalError, ex.toString(), ex));
      }
    }
  }

}
