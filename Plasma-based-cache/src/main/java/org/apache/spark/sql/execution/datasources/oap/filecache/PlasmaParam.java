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

package org.apache.spark.sql.execution.datasources.oap.filecache;

public class PlasmaParam {
  private byte[] objectId;
  private int timeoutMs = -1;
  private boolean isMetadata = false;
  private int length = 0;

  // seal contains delete
  public PlasmaParam(byte[] objectId) {
    this.objectId = objectId;
  }

  // create
  public PlasmaParam(byte[] objectId, int length) {
    this.objectId = objectId;
    this.length = length;
  }

  // getObjAsByteBuffer
  public PlasmaParam(byte[] objectId, int timeoutMs, boolean isMetadata) {
    this.objectId = objectId;
    this.timeoutMs = timeoutMs;
    this.isMetadata = isMetadata;
  }

  public byte[] getObjectId() {
    return objectId;
  }

  public int getTimeoutMs() {
    return timeoutMs;
  }

  public boolean isMetadata() {
    return isMetadata;
  }

  public void setMetadata(boolean metadata) {
    isMetadata = metadata;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }
}
