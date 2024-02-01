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
        testRescale(1, 2, 100_000_0, 10);
    }

    @Test
    public void testScaleOut_2_8() throws Exception {
        testRescale(2, 8, 100_000_0, 10);
    }

    @Test
    public void testScaleOut_2_7() throws Exception {
        testRescale(2, 7, 100_000_0, 10);
    }

    @Test
    public void testScaleIn_2_1() throws Exception {
        testRescale(2, 1, 100_000_0, 10);
    }

    @Test
    public void testScaleIn_8_2() throws Exception {
        testRescale(8, 2, 100_000_0, 10);
    }

    @Test
    public void testScaleIn_7_2() throws Exception {
        testRescale(7, 2, 100_000_0, 10);
    }

    @Test
    public void testScaleIn_2_3() throws Exception {
        testRescale(2, 3, 100_000_0, 10);
    }

    @Test
    public void testScaleIn_3_2() throws Exception {
        testRescale(3, 2, 100_000_0, 10);
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
                snapshotAllBackends(backends, snapshotResults);
            } finally {
                for (RocksDBKeyedStateBackend<Integer> backend : backends) {
                    IOUtils.closeQuietly(backend);
                    backend.dispose();
                }
                valueStates.clear();
                backends.clear();
            }

            List<KeyedStateHandle> stateHandles =
                    extractKeyedStateHandlesFromSnapshotResult(snapshotResults);

            List<KeyGroupRange> ranges = computeKeyGroupRanges(restoreParallelism, maxParallelism);

            List<List<KeyedStateHandle>> handlesByInstance =
                    computeHandlesByInstance(stateHandles, ranges, restoreParallelism);

            System.out.println(
                    "Sum of snapshot sizes: "
                            + stateHandles.stream().mapToLong(StateObject::getStateSize).sum()
                                    / (1024 * 1024)
                            + " MB");

            for (boolean useIngest : Arrays.asList(Boolean.FALSE)) {
                try {
                    System.out.println("Restoring using ingest db=" + useIngest + "... ");
                    long maxInstanceTime = Long.MIN_VALUE;
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

                        long instanceTime = System.currentTimeMillis() - tInstance;
                        if (instanceTime > maxInstanceTime) {
                            maxInstanceTime = instanceTime;
                        }

                        System.out.println(
                                "    Restored instance "
                                        + i
                                        + " from "
                                        + instanceHandles.size()
                                        + " state handles"
                                        + " time (ms): "
                                        + instanceTime);
                        backends.add(backend);
                    }
                    System.out.println(
                            "Total restore time (ms): " + (System.currentTimeMillis() - t));
                    System.out.println("Max restore time (ms): " + maxInstanceTime);

                    ///
                    for (SnapshotResult<KeyedStateHandle> snapshotResult : snapshotResults) {
                        snapshotResult.discardState();
                    }
                    snapshotResults.clear();

                    snapshotAllBackends(backends, snapshotResults);

                    int count = 0;
                    for (RocksDBKeyedStateBackend<Integer> backend : backends) {
                        count += backend.getKeys(stateName, VoidNamespace.INSTANCE).count();
                        IOUtils.closeQuietly(backend);
                        backend.dispose();
                    }
                    Assertions.assertEquals(numKeys, count);
                    backends.clear();

                    stateHandles = extractKeyedStateHandlesFromSnapshotResult(snapshotResults);
                    ranges = computeKeyGroupRanges(startParallelism, maxParallelism);
                    handlesByInstance =
                            computeHandlesByInstance(stateHandles, ranges, startParallelism);

                    System.out.println("Restoring again...");
                    maxInstanceTime = Long.MIN_VALUE;
                    t = System.currentTimeMillis();
                    for (int i = 0; i < startParallelism; ++i) {
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

                        long instanceTime = System.currentTimeMillis() - tInstance;
                        if (instanceTime > maxInstanceTime) {
                            maxInstanceTime = instanceTime;
                        }

                        System.out.println(
                                "    Restored instance "
                                        + i
                                        + " from "
                                        + instanceHandles.size()
                                        + " state handles"
                                        + " time (ms): "
                                        + instanceTime);
                        backends.add(backend);
                    }
                    System.out.println(
                            "Total restore time (ms): " + (System.currentTimeMillis() - t));
                    System.out.println("Max restore time (ms): " + maxInstanceTime);

                    count = 0;
                    for (RocksDBKeyedStateBackend<Integer> backend : backends) {
                        count += backend.getKeys(stateName, VoidNamespace.INSTANCE).count();
                    }
                    Assertions.assertEquals(numKeys, count);

                } finally {
                    for (RocksDBKeyedStateBackend<Integer> backend : backends) {
                        IOUtils.closeQuietly(backend);
                        backend.dispose();
                    }
                    backends.clear();
                }
            }
        } finally {
            for (SnapshotResult<KeyedStateHandle> snapshotResult : snapshotResults) {
                snapshotResult.discardState();
            }
        }
    }

    private void snapshotAllBackends(
            List<RocksDBKeyedStateBackend<Integer>> backends,
            List<SnapshotResult<KeyedStateHandle>> snapshotResultsOut)
            throws Exception {
        for (int i = 0; i < backends.size(); ++i) {
            RocksDBKeyedStateBackend<Integer> backend = backends.get(i);
            FsCheckpointStreamFactory fsCheckpointStreamFactory =
                    new FsCheckpointStreamFactory(
                            getSharedInstance(),
                            fromLocalFile(
                                    TempDirUtils.newFolder(
                                            tempFolder, "checkpointsDir_" + UUID.randomUUID() + i)),
                            fromLocalFile(
                                    TempDirUtils.newFolder(
                                            tempFolder, "sharedStateDir_" + UUID.randomUUID() + i)),
                            1,
                            4096);

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    backend.snapshot(
                            0L,
                            0L,
                            fsCheckpointStreamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            snapshot.run();
            snapshotResultsOut.add(snapshot.get());
        }
    }

    private List<KeyedStateHandle> extractKeyedStateHandlesFromSnapshotResult(
            List<SnapshotResult<KeyedStateHandle>> snapshotResults) {
        return snapshotResults.stream()
                .map(SnapshotResult::getJobManagerOwnedSnapshot)
                .collect(Collectors.toList());
    }

    private List<KeyGroupRange> computeKeyGroupRanges(int restoreParallelism, int maxParallelism) {
        List<KeyGroupRange> ranges = new ArrayList<>(restoreParallelism);
        for (int i = 0; i < restoreParallelism; ++i) {
            ranges.add(
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            maxParallelism, restoreParallelism, i));
        }
        return ranges;
    }

    private List<List<KeyedStateHandle>> computeHandlesByInstance(
            List<KeyedStateHandle> stateHandles,
            List<KeyGroupRange> computedRanges,
            int restoreParallelism) {
        List<List<KeyedStateHandle>> handlesByInstance = new ArrayList<>(restoreParallelism);
        for (KeyGroupRange targetRange : computedRanges) {
            List<KeyedStateHandle> handlesForTargetRange = new ArrayList<>(1);
            handlesByInstance.add(handlesForTargetRange);

            for (KeyedStateHandle stateHandle : stateHandles) {
                if (stateHandle.getKeyGroupRange().getIntersection(targetRange)
                        != KeyGroupRange.EMPTY_KEY_GROUP_RANGE) {
                    handlesForTargetRange.add(stateHandle);
                }
            }
        }
        return handlesByInstance;
    }
}
