/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connectors.hive.ApeHiveDynamicTableFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.factories.Factory;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** A catalog implementation for Hive. */
public class ApeHiveCatalog extends HiveCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(ApeHiveCatalog.class);

    private final HiveConf hiveConf;

    @VisibleForTesting HiveMetastoreClientWrapper client;

    public ApeHiveCatalog(
            String catalogName, @Nullable String defaultDatabase, @Nullable String hiveConfDir) {
        this(catalogName, defaultDatabase, hiveConfDir, null);
    }

    public ApeHiveCatalog(
            String catalogName,
            @Nullable String defaultDatabase,
            @Nullable String hiveConfDir,
            String hiveVersion) {
        this(catalogName, defaultDatabase, hiveConfDir, null, hiveVersion);
    }

    public ApeHiveCatalog(
            String catalogName,
            @Nullable String defaultDatabase,
            @Nullable String hiveConfDir,
            @Nullable String hadoopConfDir,
            @Nullable String hiveVersion) {
        this(catalogName, defaultDatabase, createHiveConf(hiveConfDir, hadoopConfDir), hiveVersion);
    }

    public ApeHiveCatalog(
            String catalogName,
            @Nullable String defaultDatabase,
            @Nullable HiveConf hiveConf,
            @Nullable String hiveVersion) {
        this(
                catalogName,
                defaultDatabase == null ? DEFAULT_DB : defaultDatabase,
                hiveConf,
                isNullOrWhitespaceOnly(hiveVersion) ? HiveShimLoader.getHiveVersion() : hiveVersion,
                false);
    }

    @VisibleForTesting
    protected ApeHiveCatalog(
            String catalogName,
            String defaultDatabase,
            @Nullable HiveConf hiveConf,
            String hiveVersion,
            boolean allowEmbedded) {
        super(catalogName, defaultDatabase, hiveConf, hiveVersion);

        this.hiveConf = hiveConf == null ? createHiveConf(null, null) : hiveConf;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new ApeHiveDynamicTableFactory(hiveConf));
    }
}
