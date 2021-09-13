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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.yarn.ApeYarnOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tools to compose starting command of task managers on yarn. */
public class ApeBootstrapTools {

    private static final Logger LOG = LoggerFactory.getLogger(ApeBootstrapTools.class);

    public static final String NUMA_BINDING_NODES_INDEX_CONF_KEY = "NUMA_BINDING_NODES_INDEX";

    /**
     * Generates the shell command to start a task manager.
     *
     * @param flinkConfig The Flink configuration.
     * @param tmParams Parameters for the task manager.
     * @param configDirectory The configuration directory for the flink-conf.yaml
     * @param logDirectory The log directory.
     * @param hasLogback Uses logback?
     * @param hasLog4j Uses log4j?
     * @param mainClass The main class to start with.
     * @return A String containing the task manager startup command.
     */
    public static String getTaskManagerShellCommand(
            Configuration flinkConfig,
            ContaineredTaskManagerParameters tmParams,
            String configDirectory,
            String logDirectory,
            boolean hasLogback,
            boolean hasLog4j,
            boolean hasKrb5,
            Class<?> mainClass,
            String mainArgs) {

        final Map<String, String> startCommandValues = new HashMap<>();

        List<String> numaNodeList = flinkConfig.get(ApeYarnOptions.NUMA_BINDING_NODE_LIST);
        if (numaNodeList.size() > 0
                && tmParams.taskManagerEnv().containsKey(NUMA_BINDING_NODES_INDEX_CONF_KEY)) {
            String index = tmParams.taskManagerEnv().get(NUMA_BINDING_NODES_INDEX_CONF_KEY);
            String nodes = numaNodeList.get(Integer.parseInt(index));

            startCommandValues.put(
                    "java",
                    String.format(
                            "export NUMA_BINDING_NODES_INDEX=%s; numactl --cpunodebind=%s --membind=%s $JAVA_HOME/bin/java",
                            index, nodes, nodes));
        } else {
            startCommandValues.put("java", "$JAVA_HOME/bin/java");
        }

        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                tmParams.getTaskExecutorProcessSpec();
        startCommandValues.put(
                "jvmmem", ProcessMemoryUtils.generateJvmParametersStr(taskExecutorProcessSpec));

        String javaOpts = flinkConfig.getString(CoreOptions.FLINK_JVM_OPTIONS);
        if (flinkConfig.getString(CoreOptions.FLINK_TM_JVM_OPTIONS).length() > 0) {
            javaOpts += " " + flinkConfig.getString(CoreOptions.FLINK_TM_JVM_OPTIONS);
        }

        // krb5.conf file will be available as local resource in JM/TM container
        if (hasKrb5) {
            javaOpts += " -Djava.security.krb5.conf=krb5.conf";
        }
        startCommandValues.put("jvmopts", javaOpts);

        String logging = "";
        if (hasLogback || hasLog4j) {
            logging = "-Dlog.file=" + logDirectory + "/taskmanager.log";
            if (hasLogback) {
                logging += " -Dlogback.configurationFile=file:" + configDirectory + "/logback.xml";
            }
            if (hasLog4j) {
                logging += " -Dlog4j.configuration=file:" + configDirectory + "/log4j.properties";
                logging +=
                        " -Dlog4j.configurationFile=file:" + configDirectory + "/log4j.properties";
            }
        }

        startCommandValues.put("logging", logging);
        startCommandValues.put("class", mainClass.getName());
        startCommandValues.put(
                "redirects",
                "1> "
                        + logDirectory
                        + "/taskmanager.out "
                        + "2> "
                        + logDirectory
                        + "/taskmanager.err");

        String argsStr =
                TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec)
                        + " --configDir "
                        + configDirectory;
        if (!mainArgs.isEmpty()) {
            argsStr += " " + mainArgs;
        }
        startCommandValues.put("args", argsStr);

        final String commandTemplate =
                flinkConfig.getString(
                        ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
                        ConfigConstants.DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE);
        String startCommand = BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
        LOG.debug("TaskManager start command: " + startCommand);

        return startCommand;
    }
}
