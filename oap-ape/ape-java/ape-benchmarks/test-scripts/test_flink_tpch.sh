#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters. Example: sh test_flink_tpch.sh YARN_APP_ID QUERIES PARALLELISM true|false."
    echo "Note: export env variables before testing: FLINK_DIR, TPCH_TEST_DIR, HIVE_DATABASE_NAME, RESULT_DIR"
    exit 1
fi

YARN_APP_ID=$1
QUERIES=$2
PARALLELISM=$3
ENABLE_APE=$4

USE_HIVE_METASTORE=true
USE_TABLE_STATS=true
CMD_BEFORE_QUERY=""

QUERY_DIR="$TPCH_TEST_DIR/tpch/queries"
MODIFIED_QUERY_DIR="$TPCH_TEST_DIR/tpch/modified-queries"

TARGET_DIR="$TPCH_TEST_DIR/target"
TABLE_DIR="$TARGET_DIR/table"

hdfs dfs -mkdir -p "$RESULT_DIR"

$FLINK_DIR/bin/flink run -t yarn-session -Dyarn.application.id=$YARN_APP_ID \
    -p $PARALLELISM -c org.apache.flink.table.tpch.TpchTestProgram "$TARGET_DIR/TpchTestProgram.jar" \
    -sourceTablePath "$TABLE_DIR" -queryPath "$QUERY_DIR" -sinkTablePath "$RESULT_DIR" -useTableStats "$USE_TABLE_STATS" \
    -useHiveMetaStore "$USE_HIVE_METASTORE" -hiveConfDir $FLINK_DIR/conf -hiveDatabaseName "$HIVE_DATABASE_NAME"\
      -cmdBeforeQuery "$CMD_BEFORE_QUERY" -sourceParallelism $PARALLELISM -querySet $QUERIES -enableApe $ENABLE_APE
