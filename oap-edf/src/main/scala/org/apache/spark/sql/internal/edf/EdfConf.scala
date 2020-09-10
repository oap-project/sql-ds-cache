/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.internal.edf

object EdfConf {

  val EDF_EXTERNAL_DB =
    SqlConfAdapter.buildConf("spark.sql.edf.externalDB")
    .internal()
    .doc("ExternalDB used to store cache meta info, now support Redis/Etcd")
    .stringConf
    .createWithDefault("RedisClient")

    val EDF_EXTERNAL_DB_SERVER =
      SqlConfAdapter.buildConf("spark.sql.edf.externalDB.server")
      .internal()
      .doc("ExternalDB's server address")
      .stringConf
      .createWithDefault("127.0.0.1")

    val EDF_EXTERNAL_DB_PORT =
      SqlConfAdapter.buildConf("spark.sql.edf.externalDB.port")
      .internal()
      .doc("ExternalDB's port")
      .stringConf
      .createWithDefault("8080")
}
