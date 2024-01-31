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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.core.fs.local.LocalFileSystem.getSharedInstance;

/** Rescaling microbenchmark for clip/ingest DB. */
public class RocksDBRecoveryTest {

    @TempDir private static java.nio.file.Path tempFolder;

    @Test
    public void testScaleOut_1_2() throws Exception {
        testRescale(1, 2, 100_000_000, 10);
    }

    @Test
    public void testScaleOut_2_8() throws Exception {
        testRescale(2, 8, 100_000_000, 10);
    }

    @Test
    public void testScaleIn_2_1() throws Exception {
        testRescale(2, 1, 100_000_000, 10);
    }

    @Test
    public void testScaleIn_8_2() throws Exception {
        testRescale(8, 2, 100_000_000, 10);
    }

    @Test
    public void testScaleIn_2_3() throws Exception {
        testRescale(2, 3, 100_000_000, 10);
    }

    @Test
    public void testScaleIn_3_2() throws Exception {
        testRescale(3, 2, 100_000_000, 10);
    }

    public void testRescale(
            int startParallelism, int restoreParallelism, int numKeys, int updateDistance)
            throws Exception {

        System.out.println(
                "Rescaling from " + startParallelism + " to " + restoreParallelism + "...");
        final String stateName = "TestValueState";
        final int maxParallelism = startParallelism * restoreParallelism;
        final List<RocksDBKeyedStateBackend<Integer>> backends = new ArrayList<>(maxParallelism);
        final List<SnapshotResult<KeyedStateHandle>> snapshotResults =
                new ArrayList<>(startParallelism);
        try {
            final List<ValueState<Integer>> valueStates = new ArrayList<>(maxParallelism);
            try {
                ValueStateDescriptor<Integer> stateDescriptor =
                        new ValueStateDescriptor<>(stateName, IntSerializer.INSTANCE);

                for (int i = 0; i < startParallelism; ++i) {
                    RocksDBKeyedStateBackend<Integer> backend =
                            RocksDBTestUtils.builderForTestDefaults(
                                            TempDirUtils.newFolder(tempFolder),
                                            IntSerializer.INSTANCE,
                                            maxParallelism,
                                            KeyGroupRangeAssignment
                                                    .computeKeyGroupRangeForOperatorIndex(
                                                            maxParallelism, startParallelism, i),
                                            Collections.emptyList())
                                    .setEnableIncrementalCheckpointing(true)
                                    .setUseIngestDbRestoreMode(true)
                                    .build();

                    valueStates.add(
                            backend.getOrCreateKeyedState(
                                    VoidNamespaceSerializer.INSTANCE, stateDescriptor));

                    backends.add(backend);
                }

                System.out.println("Inserting " + numKeys + " keys...");

                for (int i = 1; i <= numKeys; ++i) {
                    int key = i;
                    int index =
                            KeyGroupRangeAssignment.assignKeyToParallelOperator(
                                    key, maxParallelism, startParallelism);
                    backends.get(index).setCurrentKey(key);
                    valueStates.get(index).update(i);

                    if (updateDistance > 0 && i % updateDistance == 0) {
                        key = i - updateDistance + 1;
                        index =
                                KeyGroupRangeAssignment.assignKeyToParallelOperator(
                                        key, maxParallelism, startParallelism);
                        backends.get(index).setCurrentKey(key);
                        valueStates.get(index).update(i);
                    }
                }

                System.out.println("Creating snapshots...");

                for (int i = 0; i < backends.size(); ++i) {
                    RocksDBKeyedStateBackend<Integer> backend = backends.get(i);
                    FsCheckpointStreamFactory fsCheckpointStreamFactory =
                            new FsCheckpointStreamFactory(
                                    getSharedInstance(),
                                    fromLocalFile(
                                            TempDirUtils.newFolder(
                                                    tempFolder,
                                                    "checkpointsDir_" + UUID.randomUUID() + i)),
                                    fromLocalFile(
                                            TempDirUtils.newFolder(
                                                    tempFolder,
                                                    "sharedStateDir_" + UUID.randomUUID() + i)),
                                    1,
                                    4096);

                    RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                            backend.snapshot(
                                    0L,
                                    0L,
                                    fsCheckpointStreamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation());

                    snapshot.run();
                    snapshotResults.add(snapshot.get());
                }

            } finally {
                for (RocksDBKeyedStateBackend<Integer> backend : backends) {
                    IOUtils.closeQuietly(backend);
                    backend.dispose();
                }
                valueStates.clear();
                backends.clear();
            }

            List<KeyedStateHandle> stateHandles =
                    snapshotResults.stream()
                            .map(SnapshotResult::getJobManagerOwnedSnapshot)
                            .collect(Collectors.toList());

            System.out.println(
                    "Sum of snapshot sizes: "
                            + stateHandles.stream().mapToLong(StateObject::getStateSize).sum()
                                    / (1024 * 1024)
                            + " MB");

            List<KeyGroupRange> ranges = new ArrayList<>(restoreParallelism);
            for (int i = 0; i < restoreParallelism; ++i) {
                ranges.add(
                        KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                                maxParallelism, restoreParallelism, i));
            }

            List<List<KeyedStateHandle>> handlesByInstance = new ArrayList<>(restoreParallelism);
            for (KeyGroupRange targetRange : ranges) {
                List<KeyedStateHandle> handlesForTargetRange = new ArrayList<>(1);
                handlesByInstance.add(handlesForTargetRange);

                for (KeyedStateHandle stateHandle : stateHandles) {
                    if (stateHandle.getKeyGroupRange().getIntersection(targetRange)
                            != KeyGroupRange.EMPTY_KEY_GROUP_RANGE) {
                        handlesForTargetRange.add(stateHandle);
                    }
                }
            }

            for (boolean useIngest : Arrays.asList(Boolean.FALSE, Boolean.TRUE)) {
                try {
                    System.out.println("Restoring using ingest db=" + useIngest + "... ");
                    long t = System.currentTimeMillis();
                    for (int i = 0; i < restoreParallelism; ++i) {
                        List<KeyedStateHandle> instanceHandles = handlesByInstance.get(i);
                        long tInstance = System.currentTimeMillis();
                        RocksDBKeyedStateBackend<Integer> backend =
                                RocksDBTestUtils.builderForTestDefaults(
                                                TempDirUtils.newFolder(tempFolder),
                                                IntSerializer.INSTANCE,
                                                maxParallelism,
                                                ranges.get(i),
                                                instanceHandles)
                                        .setEnableIncrementalCheckpointing(true)
                                        .setUseIngestDbRestoreMode(useIngest)
                                        .build();
                        System.out.println(
                                "    Restored instance "
                                        + i
                                        + " from "
                                        + instanceHandles.size()
                                        + " state handles"
                                        + " time (ms): "
                                        + (System.currentTimeMillis() - tInstance));
                        backends.add(backend);
                    }
                    System.out.println(
                            "Total restore time (ms): " + (System.currentTimeMillis() - t));
                } finally {
                    int count = 0;
                    for (RocksDBKeyedStateBackend<Integer> backend : backends) {
                        count += backend.getKeys(stateName, VoidNamespace.INSTANCE).count();
                        IOUtils.closeQuietly(backend);
                        backend.dispose();
                    }
                    Assertions.assertEquals(numKeys, count);
                    backends.clear();
                }
            }
        } finally {
            for (SnapshotResult<KeyedStateHandle> snapshotResult : snapshotResults) {
                snapshotResult.discardState();
            }
        }
    }
}
