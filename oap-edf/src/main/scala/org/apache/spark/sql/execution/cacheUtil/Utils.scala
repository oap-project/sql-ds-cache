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

package org.apache.spark.sql.execution.cacheUtil

import com.google.common.hash.{HashFunction, Hashing}

class ObjectId(key: String) {
  override def toString: String = key

  def toByteArray(): Array[Byte] = {
    HashHelper.hash(key)
  }
}

// HashHelper will use murmur3_128 hash algorithm to calculate a 128 bit(16 bytes) result and
// put the result to a 20 bytes Array[Byte], which means the last 4 bytes will be 0.
// TODO: We can use Hashing.goodFastHash(160) to return 20 bytes, is it necessary?
object HashHelper {
  val hf: HashFunction = Hashing.murmur3_128()

  def hash(key: Array[Byte]): Array[Byte] = {
    val ret = new Array[Byte](20)
    hf.newHasher().putBytes(key).hash().writeBytesTo(ret, 0, 20)
    ret
  }

  def hash(key: String): Array[Byte] = {
    hash(key.getBytes())
  }
}

class CacheManagerException(message: String) extends RuntimeException(message) {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  def this() {
    this(null: String)
  }

}
