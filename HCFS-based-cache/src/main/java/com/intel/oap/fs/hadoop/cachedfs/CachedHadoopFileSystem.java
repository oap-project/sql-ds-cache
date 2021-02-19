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

package com.intel.oap.fs.hadoop.cachedfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * The implementation of Hadoop AbstractFileSystem. The implementation delegates to the
 * existing {@link CachedFileSystem} and is only necessary for use with
 * Hadoop 2.x. Configuration example in Hadoop core-site.xml file:
 * <pre>
 * &lt;property&gt;
 *    &lt;name>fs.AbstractFileSystem.cachedFs.impl&lt;/name&gt;
 *    &lt;value>com.intel.oap.fs.hadoop.cachedfs.CachedHadoopFileSystem&lt;/value&gt;
 * &lt;/property&gt;
 * </pre>
 *
 * The above configuration is used when you want to start a hadoop cluster after changing the 'fs.defaultFS' to
 * the new schema started with 'cachedFs' in core-site.xml
 *
 */
public class CachedHadoopFileSystem extends DelegateToFileSystem {
    /**
     * This constructor has the signature needed by
     * {@link org.apache.hadoop.fs.AbstractFileSystem#createFileSystem(URI, Configuration)}
     * in Hadoop 2.x.
     *
     * @param uri the uri for this Alluxio filesystem
     * @param conf Hadoop configuration
     * @throws URISyntaxException if <code>uri</code> has syntax error
     */
    protected CachedHadoopFileSystem(final URI uri, final Configuration conf)
            throws IOException, URISyntaxException {
        super(uri, new CachedFileSystem(), conf, Constants.CACHED_FS_SCHEME, false);
    }
}
