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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.yarn.ApeYarnOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.clusterframework.ApeBootstrapTools.NUMA_BINDING_NODES_INDEX_CONF_KEY;
import static org.apache.flink.runtime.security.ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
* This class is the executable entry point for the task manager in yarn or standalone mode.
* It constructs the related components (network, I/O manager, memory manager, RPC service, HA service)
* and starts them.
*/
public class ApeTaskManagerRunner {

	private static final Logger LOG = LoggerFactory.getLogger(ApeTaskManagerRunner.class);

	private static final int STARTUP_FAILURE_RETURN_CODE = 1;

	// --------------------------------------------------------------------------------------------
	//  Static entry point
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

		if (maxOpenFileHandles != -1L) {
			LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
		} else {
			LOG.info("Cannot determine the maximum number of open file descriptors");
		}

		runTaskManagerSecurely(args);
	}

	public static Configuration loadConfiguration(String[] args) throws FlinkParseException {
		return ConfigurationParserUtils.loadCommonConfiguration(args, TaskManagerRunner.class.getSimpleName());
	}

	public static void runTaskManager(Configuration configuration, PluginManager pluginManager) throws Exception {
		final TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, pluginManager, ApeTaskManagerRunner::createTaskExecutorService);

		taskManagerRunner.start();
	}

	public static void runTaskManagerSecurely(String[] args) {
		try {
			Configuration configuration = loadConfiguration(args);
			runTaskManagerSecurely(configuration);
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("TaskManager initialization failed.", strippedThrowable);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}
	}

	public static void runTaskManagerSecurely(Configuration configuration) throws Exception {
		replaceGracefulExitWithHaltIfConfigured(configuration);
		final PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
		FileSystem.initialize(configuration, pluginManager);

		SecurityUtils.install(new SecurityConfiguration(configuration));

		SecurityUtils.getInstalledContext().runSecured(() -> {
			runTaskManager(configuration, pluginManager);
			return null;
		});
	}

	// --------------------------------------------------------------------------------------------
	//  Static utilities
	// --------------------------------------------------------------------------------------------

	public static TaskManagerRunner.TaskExecutorService createTaskExecutorService(
			Configuration configuration,
			ResourceID resourceID,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			BlobCacheService blobCacheService,
			boolean localCommunicationOnly,
			ExternalResourceInfoProvider externalResourceInfoProvider,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		final TaskExecutor taskExecutor = startTaskManager(
				configuration,
				resourceID,
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				metricRegistry,
				blobCacheService,
				localCommunicationOnly,
				externalResourceInfoProvider,
				fatalErrorHandler);

		return TaskExecutorToServiceAdapter.createFor(taskExecutor);
	}

	public static TaskExecutor startTaskManager(
			Configuration configuration,
			ResourceID resourceID,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			BlobCacheService blobCacheService,
			boolean localCommunicationOnly,
			ExternalResourceInfoProvider externalResourceInfoProvider,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		checkNotNull(configuration);
		checkNotNull(resourceID);
		checkNotNull(rpcService);
		checkNotNull(highAvailabilityServices);

		LOG.info("Starting TaskManager with ResourceID: {}", resourceID.getStringWithMetadata());

		// replace temp directories with configured numa binding paths
		String numaBindingNodesIndex = System.getenv(NUMA_BINDING_NODES_INDEX_CONF_KEY);
		List<String> numaNodeList = configuration.get(ApeYarnOptions.NUMA_BINDING_NODE_LIST);
		List<String> numaPathList = configuration.get(ApeYarnOptions.NUMA_BINDING_PATH_LIST);
		if (numaBindingNodesIndex != null
				&& numaNodeList.size() > 0
				&& numaNodeList.size() == numaPathList.size()) {

			String numaPathsStr = numaPathList.get(Integer.parseInt(numaBindingNodesIndex));
			String[] numaPaths = numaPathsStr.split(",");
			if (numaPaths.length > 0) {
				configuration.set(CoreOptions.TMP_DIRS, numaPathsStr);
			}
		}

		String externalAddress = rpcService.getAddress();

		final TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);

		TaskManagerServicesConfiguration taskManagerServicesConfiguration =
			TaskManagerServicesConfiguration.fromConfiguration(
				configuration,
				resourceID,
				externalAddress,
				localCommunicationOnly,
				taskExecutorResourceSpec);

		Tuple2<TaskManagerMetricGroup, MetricGroup> taskManagerMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup(
			metricRegistry,
			externalAddress,
			resourceID,
			taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval());

		final ExecutorService ioExecutor = Executors.newFixedThreadPool(
			taskManagerServicesConfiguration.getNumIoThreads(),
			new ExecutorThreadFactory("flink-taskexecutor-io"));

		TaskManagerServices taskManagerServices = ApeTaskManagerServices.fromConfiguration(
			taskManagerServicesConfiguration,
			blobCacheService.getPermanentBlobService(),
			taskManagerMetricGroup.f1,
			ioExecutor,
			fatalErrorHandler);

		MetricUtils.instantiateFlinkMemoryMetricGroup(
			taskManagerMetricGroup.f1,
			taskManagerServices.getTaskSlotTable(),
			taskManagerServices::getManagedMemorySize);

		TaskManagerConfiguration taskManagerConfiguration =
			TaskManagerConfiguration.fromConfiguration(configuration, taskExecutorResourceSpec, externalAddress);

		String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();

		return new TaskExecutor(
			rpcService,
			taskManagerConfiguration,
			highAvailabilityServices,
			taskManagerServices,
			externalResourceInfoProvider,
			heartbeatServices,
			taskManagerMetricGroup.f0,
			metricQueryServiceAddress,
			blobCacheService,
			fatalErrorHandler,
			new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()),
			createBackPressureSampleService(configuration, rpcService.getScheduledExecutor()));
	}

	static BackPressureSampleService createBackPressureSampleService(
			Configuration configuration,
			ScheduledExecutor scheduledExecutor) {
		return new BackPressureSampleService(
			configuration.getInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES),
			Time.milliseconds(configuration.getInteger(WebOptions.BACKPRESSURE_DELAY)),
			scheduledExecutor);
	}

}
