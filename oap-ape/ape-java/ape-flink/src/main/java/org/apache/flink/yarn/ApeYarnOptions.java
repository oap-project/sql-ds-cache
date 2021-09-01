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

package org.apache.flink.yarn;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

/**
 * Definition of new options.
 */
public class ApeYarnOptions {
	public static final ConfigOption<List<String>> NUMA_BINDING_NODE_LIST =
			ConfigOptions.key("yarn.container-numa-binding.node-list")
					.stringType()
					.asList()
					.defaultValues()
					.withDescription("The numa nodes that are allowed to run task managers. " +
							"Format: node1[,node2...][;node3[,node4...]], e.g. \"1,2;3\". " +
							"The example means there are 2 numa binding nodes groups. " +
							"For the first, numa binds task manager's cpu and memory to nodes 1,2." +
							"For the second, numa binds to node 3. " +
							"If empty, task managers will not be set numa binding.");


	public static final ConfigOption<List<String>> NUMA_BINDING_PATH_LIST =
			ConfigOptions.key("yarn.container-numa-binding.path-list")
					.stringType()
					.asList()
					.defaultValues()
					.withDescription("The directories to be used only by the corresponding numa " +
							"nodes. Format: path1[,path2...][;path3[,path4...]], e.g. " +
							"/mnt/disk1,/mnt/disk2;/mnt/disk3. " +
							"The example means there are 2 numa binding nodes groups." +
							"And each group has specific paths to be used by task " +
							"manager bound to current group. " +
							"This option must be set as the same count of groups as node list or " +
							"empty then task managers will use default temp directories.");

	public static final ConfigOption<String> NUMA_BINDING_PATH_ORDERING =
			ConfigOptions.key("yarn.container-numa-binding.path-ordering")
					.stringType()
					.defaultValue("fair")
					.withDescription("The order of disks to be used by task managers, " +
							"either \"prior\" or \"fair\". The \"prior\" means that a path " +
							"will be used when its prior paths are out of space. The \"fair\" " +
							"means that the paths are used in a round-robin manner.");

	public static final ConfigOption<Integer> NUMA_BINDING_PATH_MAX_PERCENT =
			ConfigOptions.key("yarn.container-numa-binding.path-utilization-max-percentage")
					.intType()
					.defaultValue(95)
					.withDescription("The max percentage of storage utilization in percentage " +
							"allowed for numa binding paths.");

	public static final ConfigOption<Boolean> NUMA_BINDING_PATH_ENABLED =
			ConfigOptions.key("yarn.container-numa-binding.path-binding")
					.booleanType()
					.defaultValue(false)
					.withDescription("Indicator whether paths are bound to numa nodes.");
}
