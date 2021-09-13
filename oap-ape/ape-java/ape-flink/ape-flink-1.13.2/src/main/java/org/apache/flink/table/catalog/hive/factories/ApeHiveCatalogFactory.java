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

package org.apache.flink.table.catalog.hive.factories;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.ApeHiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.ApeHiveCatalogValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.catalog.hive.descriptors.ApeHiveCatalogValidator.CATALOG_HADOOP_CONF_DIR;
import static org.apache.flink.table.catalog.hive.descriptors.ApeHiveCatalogValidator.CATALOG_HIVE_CONF_DIR;
import static org.apache.flink.table.catalog.hive.descriptors.ApeHiveCatalogValidator.CATALOG_HIVE_VERSION;
import static org.apache.flink.table.catalog.hive.descriptors.ApeHiveCatalogValidator.CATALOG_TYPE_VALUE_HIVE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

/** Catalog factory for {@link ApeHiveCatalog}. */
public class ApeHiveCatalogFactory extends HiveCatalogFactory {
    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_HIVE); // ape_hive
        context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        final String defaultDatabase =
                descriptorProperties
                        .getOptionalString(CATALOG_DEFAULT_DATABASE)
                        .orElse(ApeHiveCatalog.DEFAULT_DB);

        final Optional<String> hiveConfDir =
                descriptorProperties.getOptionalString(CATALOG_HIVE_CONF_DIR);

        final Optional<String> hadoopConfDir =
                descriptorProperties.getOptionalString(CATALOG_HADOOP_CONF_DIR);

        final String version =
                descriptorProperties
                        .getOptionalString(CATALOG_HIVE_VERSION)
                        .orElse(HiveShimLoader.getHiveVersion());

        return new ApeHiveCatalog(
                name,
                defaultDatabase,
                hiveConfDir.orElse(null),
                hadoopConfDir.orElse(null),
                version);
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new ApeHiveCatalogValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
