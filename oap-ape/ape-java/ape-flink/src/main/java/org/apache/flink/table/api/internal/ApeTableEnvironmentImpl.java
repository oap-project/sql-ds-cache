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

package org.apache.flink.table.api.internal;

import org.apache.flink.table.api.ApeEnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.module.ModuleManager;

import java.util.Map;

public class ApeTableEnvironmentImpl extends TableEnvironmentImpl {

    protected ApeTableEnvironmentImpl(
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            TableConfig tableConfig,
            Executor executor,
            FunctionCatalog functionCatalog,
            Planner planner,
            boolean isStreamingMode,
            ClassLoader userClassLoader) {
        super(catalogManager, moduleManager, tableConfig, executor, functionCatalog, planner,
                isStreamingMode, userClassLoader);
    }

    public static TableEnvironmentImpl create(ApeEnvironmentSettings settings) {

        // temporary solution until FLINK-15635 is fixed
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        TableConfig tableConfig = new TableConfig();

        ModuleManager moduleManager = new ModuleManager();

        CatalogManager catalogManager = CatalogManager.newBuilder()
                .classLoader(classLoader)
                .config(tableConfig.getConfiguration())
                .defaultCatalog(
                        settings.getBuiltInCatalogName(),
                        new GenericInMemoryCatalog(
                                settings.getBuiltInCatalogName(),
                                settings.getBuiltInDatabaseName()))
                .build();

        FunctionCatalog functionCatalog = new FunctionCatalog(tableConfig, catalogManager, moduleManager);

        Map<String, String> executorProperties = settings.toExecutorProperties();
        Executor executor = ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
                .create(executorProperties);

        Map<String, String> plannerProperties = settings.toPlannerProperties();
        Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
                .create(
                        plannerProperties,
                        executor,
                        tableConfig,
                        functionCatalog,
                        catalogManager);

        return new TableEnvironmentImpl(
                catalogManager,
                moduleManager,
                tableConfig,
                executor,
                functionCatalog,
                planner,
                settings.isStreamingMode(),
                classLoader
        );
    }
}
