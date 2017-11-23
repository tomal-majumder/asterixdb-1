/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.test.dataflow;

import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallback;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGeneratorFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.EmptyComponent;

public class TestLsmBtreeIoOpCallbackFactory extends LSMBTreeIOOperationCallbackFactory {

    private static final long serialVersionUID = 1L;

    private static volatile int completedFlushes = 0;
    private static volatile int completedMerges = 0;
    private static volatile int rollbackFlushes = 0;
    private static volatile int rollbackMerges = 0;
    private static volatile int failedFlushes = 0;
    private static volatile int failedMerges = 0;

    public TestLsmBtreeIoOpCallbackFactory(ILSMComponentIdGeneratorFactory idGeneratorFactory) {
        super(idGeneratorFactory);
    }

    @Override
    public synchronized ILSMIOOperationCallback createIoOpCallback(ILSMIndex index) {
        completedFlushes = 0;
        completedMerges = 0;
        rollbackFlushes = 0;
        rollbackMerges = 0;
        // Whenever this is called, it resets the counter
        // However, the counters for the failed operations are never reset since we expect them
        // To be always 0
        return new TestLsmBtreeIoOpCallback(index, getComponentIdGenerator());
    }

    public int getTotalFlushes() {
        return completedFlushes + rollbackFlushes;
    }

    public int getTotalMerges() {
        return completedMerges + rollbackMerges;
    }

    public int getTotalIoOps() {
        return getTotalFlushes() + getTotalMerges();
    }

    public int getRollbackFlushes() {
        return rollbackFlushes;
    }

    public int getRollbackMerges() {
        return rollbackMerges;
    }

    public int getCompletedFlushes() {
        return completedFlushes;
    }

    public int getCompletedMerges() {
        return completedMerges;
    }

    public static int getFailedFlushes() {
        return failedFlushes;
    }

    public static int getFailedMerges() {
        return failedMerges;
    }

    public class TestLsmBtreeIoOpCallback extends LSMBTreeIOOperationCallback {
        public TestLsmBtreeIoOpCallback(ILSMIndex index, ILSMComponentIdGenerator idGenerator) {
            super(index, idGenerator);
        }

        @Override
        public void afterFinalize(LSMIOOperationType opType, ILSMDiskComponent newComponent) {
            super.afterFinalize(opType, newComponent);
            synchronized (TestLsmBtreeIoOpCallbackFactory.this) {
                if (newComponent != null) {
                    if (newComponent == EmptyComponent.INSTANCE) {
                        if (opType == LSMIOOperationType.FLUSH) {
                            rollbackFlushes++;
                        } else {
                            rollbackMerges++;
                        }
                    } else {
                        if (opType == LSMIOOperationType.FLUSH) {
                            completedFlushes++;
                        } else {
                            completedMerges++;
                        }
                    }
                } else {
                    recordFailure(opType);
                }
                TestLsmBtreeIoOpCallbackFactory.this.notifyAll();
            }
        }

        private void recordFailure(LSMIOOperationType opType) {
            if (opType == LSMIOOperationType.FLUSH) {
                failedFlushes++;
            } else {
                failedMerges++;
            }
        }
    }
}
