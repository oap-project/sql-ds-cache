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

package com.intel.oap.ape.service.netty;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import com.google.common.hash.Hashing;
import com.intel.ape.util.ConsistentHash;
import com.intel.oap.ape.service.netty.client.NettyClient;
import com.intel.oap.ape.service.netty.client.ParquetDataRequestClient;
import com.intel.oap.ape.service.netty.client.ParquetDataRequestClientFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class to choose and to connect APE servers for remote parquet data loading.
 */
public class NettyParquetRequestHelper {
    private static final Logger LOG = LoggerFactory.getLogger(NettyParquetRequestHelper.class);

    public static final String CONF_KEY_CLIENT_TIMEOUT = "fs.ape.client.timeout.seconds";
    public static final String CONF_KEY_CLIENT_THREADS = "fs.ape.client.threads";
    public static final String CONF_KEY_SERVER_LIST = "fs.ape.client.remote.servers";

    private final Configuration hadoopConfig;
    private NettyClient nettyClient;
    private Map<String, List<RemoteServer>> remoteServerGroups;
    private ConsistentHash<String> remoteServerHosts;

    static class RemoteServer {
        private final String host;
        private final int port;

        RemoteServer(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    static class ParquetSplitRequest {
        private final String fileName;
        private final String hdfsHost;
        private final int hdfsPort;
        private final long splitOffset;
        private final long splitLength;

        ParquetSplitRequest(String fileName, String hdfsHost, int hdfsPort, long splitOffset,
                                   long splitLength) {
            this.fileName = fileName;
            this.hdfsHost = hdfsHost;
            this.hdfsPort = hdfsPort;
            this.splitOffset = splitOffset;
            this.splitLength = splitLength;
        }

        @Override
        public String toString() {
            return "ParquetSplitRequest{" +
                    "fileName='" + fileName + '\'' +
                    ", hdfsHost='" + hdfsHost + '\'' +
                    ", hdfsPort=" + hdfsPort +
                    ", splitOffset=" + splitOffset +
                    ", splitLength=" + splitLength +
                    '}';
        }
    }

    public NettyParquetRequestHelper(Configuration hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
        initializeNettyClient();
        initializeServersHash();
    }

    /**
     * Create a request client to remote server considering load balance.
     * @return ParquetDataRequestClient
     */
    public ParquetDataRequestClient createRequestClient(
            Path filePath, long splitOffset, long splitLength)
            throws IOException, InterruptedException {

        // create a new netty client if the before one has been shut down.
        if (nettyClient == null) {
            initializeNettyClient();
        }

        // construct data request object
        final String fileName = filePath.toUri().getRawPath();
        final String defaultFs = hadoopConfig.get("fs.defaultFS");
        String[] defaultFsParts = defaultFs.split(":");
        String hdfsHost = filePath.toUri().getHost() != null ?
                filePath.toUri().getHost() : defaultFsParts[1].substring(2);
        int hdfsPort = filePath.toUri().getPort() != -1 ?
                filePath.toUri().getPort() : Integer.parseInt(defaultFsParts[2]);
        ParquetSplitRequest request = new ParquetSplitRequest(
                fileName,
                hdfsHost,
                hdfsPort,
                splitOffset,
                splitLength
        );

        RemoteServer remoteServer = selectRemoteServer(request);
        LOG.info("Creating request client to server: {}", remoteServer.toString());

        // start and return a new request client
        ParquetDataRequestClientFactory factory =
                new ParquetDataRequestClientFactory(nettyClient);
        return factory.createParquetDataRequestClient(remoteServer.host, remoteServer.port);
    }

    private void initializeNettyClient() {
        // get configurations
        int timeoutSeconds = hadoopConfig.getInt(CONF_KEY_CLIENT_TIMEOUT, 120);
        // in APE, a netty client is not shared across tasks. So it does not need a large
        // thread pool to handle server responses.
        int clientThreads = hadoopConfig.getInt(CONF_KEY_CLIENT_THREADS, 1);


        if (timeoutSeconds < 0) {
            throw new InvalidParameterException("Negative client timeout is not allowed.");
        }

        if (clientThreads <= 0) {
            throw new InvalidParameterException("Client threads should be greater than 0.");
        }

        nettyClient = new NettyClient(timeoutSeconds, clientThreads);
    }

    private void initializeServersHash() {
        // available servers
        String serversConf = hadoopConfig.get(CONF_KEY_SERVER_LIST, "");
        if (serversConf.isEmpty()) {
            throw new RuntimeException("No available servers for parquet data loading.");
        }

        // parse remote servers
        remoteServerGroups =
                Arrays.stream(serversConf.split(","))
                        .distinct()
                        .map(s -> s.split(":"))
                        .filter(s -> s.length == 2)
                        .map(s -> new RemoteServer(s[0], Integer.parseInt(s[1])) )
                        .collect(Collectors.groupingBy(s -> s.host));

        remoteServerHosts = new ConsistentHash<>(
                Hashing.sha256(),
                remoteServerGroups.keySet()
        );
    }

    private RemoteServer selectRemoteServer(ParquetSplitRequest request) {
        // select a host with consistent hash
        String selectedHost = remoteServerHosts.get(request);

        // select a server on chosen host randomly
        List<RemoteServer> serversOnSelectedHost = remoteServerGroups.get(selectedHost);
        return serversOnSelectedHost.get(
                new Random().nextInt(serversOnSelectedHost.size()));
    }

    public void shutdownNettyClient() {
        if (nettyClient != null) {
            nettyClient.shutdown();
            nettyClient = null;
        }
    }

}
