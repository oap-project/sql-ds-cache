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
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.connectors.hive.ApeHiveDynamicTableFactory;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig$;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.getHadoopConfiguration;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
 * A catalog implementation for Hive.
 */
public class ApeHiveCatalog extends HiveCatalog {

	private static final Logger LOG = LoggerFactory.getLogger(ApeHiveCatalog.class);

	private final HiveConf hiveConf;

	@VisibleForTesting
	HiveMetastoreClientWrapper client;

	public ApeHiveCatalog(String catalogName, @Nullable String defaultDatabase, @Nullable String hiveConfDir) {
		this(catalogName, defaultDatabase, hiveConfDir, null);
	}

	public ApeHiveCatalog(String catalogName, @Nullable String defaultDatabase, @Nullable String hiveConfDir, String hiveVersion) {
		this(catalogName, defaultDatabase, hiveConfDir, null, hiveVersion);
	}

	public ApeHiveCatalog(String catalogName, @Nullable String defaultDatabase, @Nullable String hiveConfDir, @Nullable String hadoopConfDir, @Nullable String hiveVersion) {
		this(catalogName, defaultDatabase, createHiveConf(hiveConfDir, hadoopConfDir), hiveVersion);
	}

	public ApeHiveCatalog(String catalogName, @Nullable String defaultDatabase, @Nullable HiveConf hiveConf, @Nullable String hiveVersion) {
		this(catalogName,
			defaultDatabase == null ? DEFAULT_DB : defaultDatabase,
			hiveConf,
			isNullOrWhitespaceOnly(hiveVersion) ? HiveShimLoader.getHiveVersion() : hiveVersion,
			false);
	}

	@VisibleForTesting
	protected ApeHiveCatalog(String catalogName, String defaultDatabase, @Nullable HiveConf hiveConf, String hiveVersion,
			boolean allowEmbedded) {
		super(catalogName, defaultDatabase, hiveConf, hiveVersion);

		this.hiveConf = hiveConf == null ? createHiveConf(null, null) : hiveConf;
	}

	@Override
	public Optional<Factory> getFactory() {
		return Optional.of(new ApeHiveDynamicTableFactory(hiveConf));
	}

	private static HiveConf createHiveConf(@Nullable String hiveConfDir, @Nullable String hadoopConfDir) {
		// create HiveConf from hadoop configuration with hadoop conf directory configured.
		Configuration hadoopConf = null;
		if (isNullOrWhitespaceOnly(hadoopConfDir)) {
			for (String possibleHadoopConfPath : HadoopUtils.possibleHadoopConfPaths(new org.apache.flink.configuration.Configuration())) {
				hadoopConf = getHadoopConfiguration(possibleHadoopConfPath);
				if (hadoopConf != null) {
					break;
				}
			}
		} else {
			hadoopConf = getHadoopConfiguration(hadoopConfDir);
		}
		if (hadoopConf == null) {
			hadoopConf = new Configuration();
		}
		HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);

		LOG.info("Setting hive conf dir as {}", hiveConfDir);

		if (hiveConfDir != null) {
			Path hiveSite = new Path(hiveConfDir, "hive-site.xml");
			if (!hiveSite.toUri().isAbsolute()) {
				// treat relative URI as local file to be compatible with previous behavior
				hiveSite = new Path(new File(hiveSite.toString()).toURI());
			}
			try (InputStream inputStream = hiveSite.getFileSystem(hadoopConf).open(hiveSite)) {
				hiveConf.addResource(inputStream, hiveSite.toString());
				// trigger a read from the conf so that the input stream is read
				isEmbeddedMetastore(hiveConf);
			} catch (IOException e) {
				throw new CatalogException("Failed to load hive-site.xml from specified path:" + hiveSite, e);
			}
		}
		return hiveConf;
	}

}
