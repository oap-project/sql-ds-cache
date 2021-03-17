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

import java.util.concurrent.*;

import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.arrow.plasma.exceptions.PlasmaClientException;
import org.apache.arrow.plasma.exceptions.PlasmaGetException;

/**
 * Plasma Server store may dead or no response during the runtime
 * Wrapper plasmaClient methods with a timeOut mechanism
 */
public class PlasmaTimeOutWrapper implements Callable<Object> {
    private ExecutorService executorService;
    private long timeOutInSeconds;
    private PlasmaMethodWrapper wrapper;
    private PlasmaClient client;
    private PlasmaParam plasmaParam;

    private PlasmaTimeOutWrapper() {
    }

    public static Object run(
            PlasmaMethodWrapper wrapper,
            PlasmaClient client,
            ExecutorService executorService,
            PlasmaParam plasmaParam, long timeOutInSeconds)
            throws InterruptedException, ExecutionException, TimeoutException,
            DuplicateObjectException, PlasmaGetException, PlasmaClientException {
        PlasmaTimeOutWrapper plasmaTimeOutWrapper = new PlasmaTimeOutWrapper();
        return plasmaTimeOutWrapper
                .submitFutureTask(wrapper, client, executorService, plasmaParam, timeOutInSeconds);
    }

    private Object submitFutureTask(
            PlasmaMethodWrapper wrapper,
            PlasmaClient client,
            ExecutorService executorService,
            PlasmaParam plasmaParam, long timeOutInSeconds)
            throws InterruptedException, ExecutionException, TimeoutException,
            DuplicateObjectException, PlasmaGetException, PlasmaClientException {
        this.client = client;
        this.wrapper = wrapper;
        this.executorService = executorService;
        this.plasmaParam = plasmaParam;
        this.timeOutInSeconds = timeOutInSeconds;
        FutureTask<Object> futureTask = (FutureTask<Object>) executorService.submit(this);
        executorService.execute(futureTask);
        return futureTask.get(timeOutInSeconds, TimeUnit.SECONDS);
    }

    @Override
    public Object call() {
        return wrapper.execute(client, plasmaParam);
    }
}
