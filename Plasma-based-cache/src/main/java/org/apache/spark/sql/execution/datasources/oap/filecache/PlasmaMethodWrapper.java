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

import org.apache.arrow.plasma.PlasmaClient;

interface PlasmaMethodWrapper {
    Object execute(PlasmaClient client, PlasmaParam plasmaParam);
}

class PlasmaCreate implements PlasmaMethodWrapper {

    @Override
    public Object execute(PlasmaClient client, PlasmaParam plasmaParam) {
        return client.create(plasmaParam.getObjectId(), plasmaParam.getLength());
    }
}

class PlasmaSeal implements PlasmaMethodWrapper {

    @Override
    public Object execute(PlasmaClient client, PlasmaParam plasmaParam) {
        client.seal(plasmaParam.getObjectId());
        return new Object();
    }
}

class PlasmaDelete implements PlasmaMethodWrapper {

    @Override
    public Object execute(PlasmaClient client, PlasmaParam plasmaParam) {
        client.delete(plasmaParam.getObjectId());
        return new Object();
    }
}

class PlasmaContains implements PlasmaMethodWrapper {

    @Override
    public Object execute(PlasmaClient client, PlasmaParam plasmaParam) {
        return client.contains(plasmaParam.getObjectId());
    }
}


class PlasmaGetObjAsByteBuffer implements PlasmaMethodWrapper {

    @Override
    public Object execute(PlasmaClient client, PlasmaParam plasmaParam) {
        return client.getObjAsByteBuffer(plasmaParam.getObjectId(),
                plasmaParam.getTimeoutMs(),
                plasmaParam.isMetadata());
    }
}
