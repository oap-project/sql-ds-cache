/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License; Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing; software
 * distributed under the License is distributed on an "AS IS" BASIS;
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND; either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.ape.service.params;

import java.io.Serializable;
import java.util.List;

import org.apache.parquet.schema.PrimitiveType;

/**
 * This class holds params needed to initialize a native parquet reader on remote server.
 * Params will be set in {@link com.intel.ape.service.netty.NettyMessage.ParquetReaderInitRequest}
 */
public class ParquetReaderInitParams implements Serializable {
    private String fileName;
    private String hdfsHost;
    private int hdfsPort;

    private String jsonSchema;
    private int firstRowGroupIndex;
    private int totalGroupToRead;

    /* For buffer allocations */
    private List<PrimitiveType.PrimitiveTypeName> typeNames;
    private int batchSize;

    private boolean plasmaCacheEnabled;
    private boolean preBufferEnabled;
    private boolean plasmaCacheAsync;

    private CacheLocalityStorage cacheLocalityStorage;

    private String filterPredicate;
    private String aggregateExpression;

    @Override
    public String toString() {
        return "ParquetReaderInitParams{" +
                "fileName='" + fileName + '\'' +
                ", hdfsHost='" + hdfsHost + '\'' +
                ", hdfsPort=" + hdfsPort +
                ", jsonSchema='" + jsonSchema + '\'' +
                ", firstRowGroupIndex=" + firstRowGroupIndex +
                ", totalGroupToRead=" + totalGroupToRead +
                ", typeNames=" + typeNames +
                ", batchSize=" + batchSize +
                ", plasmaCacheEnabled=" + plasmaCacheEnabled +
                ", preBufferEnabled=" + preBufferEnabled +
                ", plasmaCacheAsync=" + plasmaCacheAsync +
                ", cacheLocalityStorage=" + cacheLocalityStorage +
                ", filterPredicate='" + filterPredicate + '\'' +
                ", aggregateExpression='" + aggregateExpression + '\'' +
                '}';
    }

    public ParquetReaderInitParams(String fileName, String hdfsHost, int hdfsPort,
                                   String jsonSchema, int firstRowGroupIndex, int totalGroupToRead,
                                   List<PrimitiveType.PrimitiveTypeName> typeNames, int batchSize,
                                   boolean plasmaCacheEnabled, boolean preBufferEnabled,
                                   boolean plasmaCacheAsync) {
        this.fileName = fileName;
        this.hdfsHost = hdfsHost;
        this.hdfsPort = hdfsPort;
        this.jsonSchema = jsonSchema;
        this.firstRowGroupIndex = firstRowGroupIndex;
        this.totalGroupToRead = totalGroupToRead;
        this.typeNames = typeNames;
        this.batchSize = batchSize;
        this.plasmaCacheEnabled = plasmaCacheEnabled;
        this.preBufferEnabled = preBufferEnabled;
        this.plasmaCacheAsync = plasmaCacheAsync;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getHdfsHost() {
        return hdfsHost;
    }

    public void setHdfsHost(String hdfsHost) {
        this.hdfsHost = hdfsHost;
    }

    public int getHdfsPort() {
        return hdfsPort;
    }

    public void setHdfsPort(int hdfsPort) {
        this.hdfsPort = hdfsPort;
    }

    public String getJsonSchema() {
        return jsonSchema;
    }

    public void setJsonSchema(String jsonSchema) {
        this.jsonSchema = jsonSchema;
    }

    public int getFirstRowGroupIndex() {
        return firstRowGroupIndex;
    }

    public void setFirstRowGroupIndex(int firstRowGroupIndex) {
        this.firstRowGroupIndex = firstRowGroupIndex;
    }

    public int getTotalGroupToRead() {
        return totalGroupToRead;
    }

    public void setTotalGroupToRead(int totalGroupToRead) {
        this.totalGroupToRead = totalGroupToRead;
    }

    public List<PrimitiveType.PrimitiveTypeName> getTypeNames() {
        return typeNames;
    }

    public void setTypeNames(List<PrimitiveType.PrimitiveTypeName> typeNames) {
        this.typeNames = typeNames;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isPlasmaCacheEnabled() {
        return plasmaCacheEnabled;
    }

    public void setPlasmaCacheEnabled(boolean plasmaCacheEnabled) {
        this.plasmaCacheEnabled = plasmaCacheEnabled;
    }

    public boolean isPreBufferEnabled() {
        return preBufferEnabled;
    }

    public void setPreBufferEnabled(boolean preBufferEnabled) {
        this.preBufferEnabled = preBufferEnabled;
    }

    public boolean isPlasmaCacheAsync() {
        return plasmaCacheAsync;
    }

    public void setPlasmaCacheAsync(boolean plasmaCacheAsync) {
        this.plasmaCacheAsync = plasmaCacheAsync;
    }

    public CacheLocalityStorage getCacheLocalityStorage() {
        return cacheLocalityStorage;
    }

    public void setCacheLocalityStorage(CacheLocalityStorage cacheLocalityStorage) {
        this.cacheLocalityStorage = cacheLocalityStorage;
    }

    public String getFilterPredicate() {
        return filterPredicate;
    }

    public void setFilterPredicate(String filterPredicate) {
        this.filterPredicate = filterPredicate;
    }

    public String getAggregateExpression() {
        return aggregateExpression;
    }

    public void setAggregateExpression(String aggregateExpression) {
        this.aggregateExpression = aggregateExpression;
    }

    static class CacheLocalityStorage implements Serializable {
        private String redisHost;
        private int redisPort;
        private String redisPassword;

        CacheLocalityStorage(String redisHost, int redisPort, String redisPassword) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
            this.redisPassword = redisPassword;
        }

        public String getRedisHost() {
            return redisHost;
        }

        public void setRedisHost(String redisHost) {
            this.redisHost = redisHost;
        }

        public int getRedisPort() {
            return redisPort;
        }

        public void setRedisPort(int redisPort) {
            this.redisPort = redisPort;
        }

        public String getRedisPassword() {
            return redisPassword;
        }

        public void setRedisPassword(String redisPassword) {
            this.redisPassword = redisPassword;
        }

        @Override
        public String toString() {
            return "CacheLocalityStorage{" +
                    "redisHost='" + redisHost + '\'' +
                    ", redisPort=" + redisPort +
                    ", redisPassword='" + redisPassword + '\'' +
                    '}';
        }
    }
}
