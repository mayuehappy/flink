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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava32.com.google.common.primitives.UnsignedBytes;

import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ExportImportFilesMetaData;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Utils for RocksDB Incremental Checkpoint. */
public class RocksDBIncrementalCheckpointUtils {
    /**
     * Evaluates state handle's "score" regarding to the target range when choosing the best state
     * handle to init the initial db for recovery, if the overlap fraction is less than
     * overlapFractionThreshold, then just return {@code Score.MIN} to mean the handle has no chance
     * to be the initial handle.
     */
    private static Score stateHandleEvaluator(
            KeyedStateHandle stateHandle,
            KeyGroupRange targetKeyGroupRange,
            double overlapFractionThreshold) {
        final KeyGroupRange handleKeyGroupRange = stateHandle.getKeyGroupRange();
        final KeyGroupRange intersectGroup =
                handleKeyGroupRange.getIntersection(targetKeyGroupRange);

        final double overlapFraction =
                (double) intersectGroup.getNumberOfKeyGroups()
                        / handleKeyGroupRange.getNumberOfKeyGroups();

        if (overlapFraction < overlapFractionThreshold) {
            return Score.MIN;
        }
        return new Score(intersectGroup.getNumberOfKeyGroups(), overlapFraction);
    }

    /**
     * Score of the state handle, intersect group range is compared first, and then compare the
     * overlap fraction.
     */
    private static class Score implements Comparable<Score> {

        public static final Score MIN = new Score(Integer.MIN_VALUE, -1.0);

        private final int intersectGroupRange;

        private final double overlapFraction;

        public Score(int intersectGroupRange, double overlapFraction) {
            this.intersectGroupRange = intersectGroupRange;
            this.overlapFraction = overlapFraction;
        }

        public int getIntersectGroupRange() {
            return intersectGroupRange;
        }

        public double getOverlapFraction() {
            return overlapFraction;
        }

        @Override
        public int compareTo(@Nonnull Score other) {
            return Comparator.comparing(Score::getIntersectGroupRange)
                    .thenComparing(Score::getOverlapFraction)
                    .compare(this, other);
        }
    }

    /**
     * The method to clip the db instance according to the target key group range using the {@link
     * RocksDB#delete(ColumnFamilyHandle, byte[])}.
     *
     * @param db the RocksDB instance to be clipped.
     * @param columnFamilyHandles the column families in the db instance.
     * @param targetKeyGroupRange the target key group range.
     * @param currentKeyGroupRange the key group range of the db instance.
     * @param keyGroupPrefixBytes Number of bytes required to prefix the key groups.
     */
    public static void clipDBWithKeyGroupRange(
            @Nonnull RocksDB db,
            @Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
            @Nonnull KeyGroupRange targetKeyGroupRange,
            @Nonnull KeyGroupRange currentKeyGroupRange,
            @Nonnegative int keyGroupPrefixBytes)
            throws RocksDBException {

        final byte[] beginKeyGroupBytes = new byte[keyGroupPrefixBytes];
        final byte[] endKeyGroupBytes = new byte[keyGroupPrefixBytes];

        if (currentKeyGroupRange.getStartKeyGroup() < targetKeyGroupRange.getStartKeyGroup()) {
            CompositeKeySerializationUtils.serializeKeyGroup(
                    currentKeyGroupRange.getStartKeyGroup(), beginKeyGroupBytes);
            CompositeKeySerializationUtils.serializeKeyGroup(
                    targetKeyGroupRange.getStartKeyGroup(), endKeyGroupBytes);
            deleteRange(db, columnFamilyHandles, beginKeyGroupBytes, endKeyGroupBytes);
        }

        if (currentKeyGroupRange.getEndKeyGroup() > targetKeyGroupRange.getEndKeyGroup()) {
            CompositeKeySerializationUtils.serializeKeyGroup(
                    targetKeyGroupRange.getEndKeyGroup() + 1, beginKeyGroupBytes);
            CompositeKeySerializationUtils.serializeKeyGroup(
                    currentKeyGroupRange.getEndKeyGroup() + 1, endKeyGroupBytes);
            deleteRange(db, columnFamilyHandles, beginKeyGroupBytes, endKeyGroupBytes);
        }
    }

    /**
     * Delete the record falls into [beginKeyBytes, endKeyBytes) of the db.
     *
     * @param db the target need to be clipped.
     * @param columnFamilyHandles the column family need to be clipped.
     * @param beginKeyBytes the begin key bytes
     * @param endKeyBytes the end key bytes
     */
    private static void deleteRange(
            RocksDB db,
            List<ColumnFamilyHandle> columnFamilyHandles,
            byte[] beginKeyBytes,
            byte[] endKeyBytes)
            throws RocksDBException {

        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            // Using RocksDB's deleteRange will take advantage of delete
            // tombstones, which mark the range as deleted.
            //
            // https://github.com/ververica/frocksdb/blob/FRocksDB-6.20.3/include/rocksdb/db.h#L363-L377
            db.deleteRange(columnFamilyHandle, beginKeyBytes, endKeyBytes);
        }
    }

    /**
     * Clips and compacts the given database to the given key-group range. Any entries outside this
     * range will be completely deleted (including tombstones).
     *
     * @param db the target need to be clipped.
     * @param columnFamilyHandles the column family need to be clipped.
     * @param keyGroupPrefixBytes the number of bytes required to represent all key-groups under the
     *     current max parallelism.
     * @param dbExpectedKeyGroupRange the key-group range the rocksdb instance.
     */
    public static void compactSstFilesToExpectedRange(
            RocksDB db,
            List<ColumnFamilyHandle> columnFamilyHandles,
            int keyGroupPrefixBytes,
            KeyGroupRange dbExpectedKeyGroupRange)
            throws Exception {

        final byte[] beginKeyGroupBytes = new byte[keyGroupPrefixBytes];
        final byte[] endKeyGroupBytes = new byte[keyGroupPrefixBytes];

        CompositeKeySerializationUtils.serializeKeyGroup(
                dbExpectedKeyGroupRange.getStartKeyGroup(), beginKeyGroupBytes);

        CompositeKeySerializationUtils.serializeKeyGroup(
                dbExpectedKeyGroupRange.getEndKeyGroup() + 1, endKeyGroupBytes);

        Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
        KeyRange dbKeyRange = getDBKeyRange(db);

        boolean clipMinRangeRequired =
                comparator.compare(dbKeyRange.minKey, beginKeyGroupBytes) < 0;
        boolean clipMaxRangeRequired = comparator.compare(dbKeyRange.maxKey, endKeyGroupBytes) >= 0;

        if (clipMinRangeRequired || clipMaxRangeRequired) {
            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                db.clipColumnFamily(columnFamilyHandle, beginKeyGroupBytes, endKeyGroupBytes);
            }

            if (clipMinRangeRequired) {
                for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                    db.compactRange(columnFamilyHandle, new byte[] {}, beginKeyGroupBytes);
                }
            }

            if (clipMaxRangeRequired) {
                for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                    db.compactRange(
                            columnFamilyHandle,
                            endKeyGroupBytes,
                            // This key is larger than the current limit for key groups
                            new byte[] {(byte) 0xFF, (byte) 0xFF});
                }
            }
        }
    }

    private static KeyRange getDBKeyRange(RocksDB db) {
        final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
        final List<LiveFileMetaData> liveFilesMetaData = db.getLiveFilesMetaData();

        if (liveFilesMetaData.isEmpty()) {
            return KeyRange.EMPTY;
        }

        Iterator<LiveFileMetaData> liveFileMetaDataIterator = liveFilesMetaData.iterator();
        LiveFileMetaData fileMetaData = liveFileMetaDataIterator.next();
        byte[] smallestKey = fileMetaData.smallestKey();
        byte[] largestKey = fileMetaData.largestKey();
        while (liveFileMetaDataIterator.hasNext()) {
            fileMetaData = liveFileMetaDataIterator.next();
            byte[] sstSmallestKey = fileMetaData.smallestKey();
            byte[] sstLargestKey = fileMetaData.largestKey();
            if (comparator.compare(sstSmallestKey, smallestKey) < 0) {
                smallestKey = sstSmallestKey;
            }
            if (comparator.compare(sstLargestKey, largestKey) > 0) {
                largestKey = sstLargestKey;
            }
        }
        return KeyRange.of(smallestKey, largestKey);
    }

    public static void exportColumnFamilies(
            RocksDB db,
            List<ColumnFamilyHandle> columnFamilyHandles,
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            Path exportBasePath,
            Map<RegisteredStateMetaInfoBase, List<ExportImportFilesMetaData>> resultOutput)
            throws RocksDBException {

        Preconditions.checkArgument(
                columnFamilyHandles.size() == stateMetaInfoSnapshots.size(),
                "Lists are aligned by index and must be of the same size!");

        try (final Checkpoint checkpoint = Checkpoint.create(db)) {
            for (int i = 0; i < columnFamilyHandles.size(); i++) {
                StateMetaInfoSnapshot metaInfoSnapshot = stateMetaInfoSnapshots.get(i);

                RegisteredStateMetaInfoBase stateMetaInfo =
                        RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(metaInfoSnapshot);

                Path subPath = exportBasePath.resolve(UUID.randomUUID().toString());
                ExportImportFilesMetaData exportedColumnFamilyMetaData =
                        checkpoint.exportColumnFamily(
                                columnFamilyHandles.get(i), subPath.toString());

                File[] exportedSstFiles =
                        subPath.toFile()
                                .listFiles((file, name) -> name.toLowerCase().endsWith(".sst"));

                if (exportedSstFiles != null && exportedSstFiles.length > 0) {
                    resultOutput
                            .computeIfAbsent(stateMetaInfo, (key) -> new ArrayList<>())
                            .add(exportedColumnFamilyMetaData);
                } else {
                    // Close unused empty export result
                    IOUtils.closeQuietly(exportedColumnFamilyMetaData);
                }
            }
        }
    }

    /** check whether the bytes is before prefixBytes in the character order. */
    public static boolean beforeThePrefixBytes(@Nonnull byte[] bytes, @Nonnull byte[] prefixBytes) {
        final int prefixLength = prefixBytes.length;
        for (int i = 0; i < prefixLength; ++i) {
            int r = (char) prefixBytes[i] - (char) bytes[i];
            if (r != 0) {
                return r > 0;
            }
        }
        return false;
    }

    /**
     * Choose the best state handle according to the {@link #stateHandleEvaluator(KeyedStateHandle,
     * KeyGroupRange, double)} to init the initial db.
     *
     * @param restoreStateHandles The candidate state handles.
     * @param targetKeyGroupRange The target key group range.
     * @return The best candidate or null if no candidate was a good fit.
     */
    @Nullable
    public static <T extends KeyedStateHandle> T chooseTheBestStateHandleForInitial(
            @Nonnull Collection<T> restoreStateHandles,
            @Nonnull KeyGroupRange targetKeyGroupRange,
            double overlapFractionThreshold) {

        // Shortcut for a common case (scale out)
        if (restoreStateHandles.size() == 1) {
            return restoreStateHandles.iterator().next();
        }

        T bestStateHandle = null;
        Score bestScore = Score.MIN;
        for (T rawStateHandle : restoreStateHandles) {
            Score handleScore =
                    stateHandleEvaluator(
                            rawStateHandle, targetKeyGroupRange, overlapFractionThreshold);
            if (bestStateHandle == null || handleScore.compareTo(bestScore) > 0) {
                bestStateHandle = rawStateHandle;
                bestScore = handleScore;
            }
        }

        return bestStateHandle;
    }

    private static final class KeyRange {
        static final KeyRange EMPTY = KeyRange.of(new byte[0], new byte[0]);

        final byte[] minKey;
        final byte[] maxKey;

        private KeyRange(byte[] minKey, byte[] maxKey) {
            this.minKey = minKey;
            this.maxKey = maxKey;
        }

        static KeyRange of(byte[] minKey, byte[] maxKey) {
            return new KeyRange(minKey, maxKey);
        }
    }
}
