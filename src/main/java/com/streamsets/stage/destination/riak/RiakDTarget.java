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

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.stage.lib.riak.RiakFieldMappingConfig;

import java.util.List;

@StageDef(
    version = 1,
    label = "Riak TS Destination",
    description = "",
    icon = "riak.png",
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class RiakDTarget extends RiakTarget {

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.LIST,
          defaultValue = "[\"localhost\"]",
          label = "Hostnames",
          description = "Hostnames of Riak cluster nodes.",
          displayPosition = 10,
          group = "RIAK"
  )
  public List<String> hostnames;

  /** {@inheritDoc} */
  @Override
  public List<String> getHostnames() {
    return hostnames;
  }

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.NUMBER,
          defaultValue = "8087",
          label = "Port",
          description = "Port number to use when connecting to Riak nodes.",
          displayPosition = 20,
          group = "RIAK"
  )
  public int port;

  /** {@inheritDoc} */
  @Override
  public int getPort() {
    return port;
  }

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
          evaluation = ConfigDef.Evaluation.EXPLICIT,
          defaultValue = "${record:attribute('tableName')}",
          label = "Table Name",
          description = "Riak TS table name.",
          displayPosition = 30,
          group = "RIAK"
  )
  public String tableNameTemplate;

  /** {@inheritDoc} */
  @Override
  public String getTableNameTemplate() {
    return tableNameTemplate;
  }

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.MODEL,
          defaultValue = "",
          label = "Field to Column Mapping",
          description = "Optionally specify additional field mappings when input field name and column name don't match.",
          displayPosition = 40,
          group = "RIAK"
  )
  @ListBeanModel
  public List<RiakFieldMappingConfig> customMappings;

  /** {@inheritDoc} */
  @Override
  public List<RiakFieldMappingConfig> getCustomMappings() {
    return customMappings;
  }

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.BOOLEAN,
          defaultValue = "true",
          label = "Use Multi-Row Insert",
          description = "Whether to generate multi-row store commans instead of multiple single-row stores",
          displayPosition = 50,
          group = "RIAK"
  )
  public boolean useMultiRowInsert;

  /** {@inheritDoc} */
  @Override
  public boolean getUseMultiRowInsert() { return useMultiRowInsert; }

}
