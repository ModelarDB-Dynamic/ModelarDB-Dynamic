/* Copyright 2018 The ModelarDB Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.aau.modelardb.core.GroupBasedCompression;

import dk.aau.modelardb.core.model.compression.ModelType;
import dk.aau.modelardb.core.model.compression.Segment;
import dk.aau.modelardb.core.utility.Static;
import dk.aau.modelardb.storage.Storage;
import scala.collection.mutable.HashMap;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class SegmentGroup {

    private final static int[] defaultDerivedTimeSeries = new int[0];
    /**
     * Instance Variables
     **/
    public final int gid;
    public final long startTime;
    public final long endTime;
    public final int samplingInterval;
    public final int mtid;
    public final byte[] model;
    public final byte[] gaps;

    /**
     * Constructors
     **/
    public SegmentGroup(int gid, long startTime, int samplingInterval, long endTime, int mtid, byte[] model, byte[] gaps) {
        this.gid = gid;
        this.startTime = startTime;
        this.endTime = endTime;
        this.samplingInterval = samplingInterval;
        this.mtid = mtid;
        this.model = model;
        this.gaps = gaps;
    }

    /**
     * Public Methods
     **/
    public String toString() {
        //The segments might not represent all time series in the time series group
        int[] gaps = Static.bytesToInts(this.gaps);
        StringBuilder sb = new StringBuilder();
        sb.append("Segment: [gid: ").append(this.gid).append(" | start: ").append(this.startTime).append(" | end: ")
                .append(this.endTime).append("| si: ").append(this.samplingInterval).append(" | mtid: ").append(this.mtid);
        if(gaps.length > 0) {
            sb.append(" | gaps: ");
            sb.append(Arrays.toString(gaps));
        }
        sb.append("]");
        return sb.toString();
    }

    //TODO use sampling interval
    public SegmentGroup[] explode(int[][] groupMetadataCache, HashMap<Integer, int[]> groupDerivedCache) {
        int[] gmc = groupMetadataCache[this.gid];
        int[] derivedTimeSeries = groupDerivedCache.getOrElse(this.gid, () -> SegmentGroup.defaultDerivedTimeSeries);
        int[] timeSeriesInAGap = Static.bytesToInts(this.gaps);
        int temporalOffset = 0;
        if (timeSeriesInAGap.length > 0 && timeSeriesInAGap[timeSeriesInAGap.length - 1] < 0) {
            //HACK: a temporal offset from START might be store at the end as a negative integer as tids are always positive
            temporalOffset = -1 * timeSeriesInAGap[timeSeriesInAGap.length - 1];
        }
        //Minus one because gmc stores the groups sampling interval at index zero
        int storedGroupSize = gmc.length - 1 - timeSeriesInAGap.length;

        //Creates a segment for all stored time series in the group that are not currently in a gap
        SegmentGroup[] segments;
        int nextSegment = 0;
        if (timeSeriesInAGap.length == 0) {
            //If no gaps exist, segments will be constructed for all stored time series and derived time series in the group
            int storedAndDerivedGroupSize = storedGroupSize + (derivedTimeSeries.length / 2);
            segments = new SegmentGroup[storedAndDerivedGroupSize];
            for (int index = 1; index < gmc.length; index++) {
                int tid = gmc[index];
                //Offsets store the following: [0] Group Offset, [1] Group Size, [2] Temporal Offset
                byte[] offset = ByteBuffer.allocate(12).putInt(nextSegment + 1).putInt(storedGroupSize).putInt(temporalOffset).array();
                segments[nextSegment] = new SegmentGroup(tid, this.startTime, this.samplingInterval, this.endTime, this.mtid, this.model, offset);
                nextSegment++;
            }
        } else {
            //If gaps exist, segments will not be constructed for time series in a gap and for time series that derive from them
            int storedAndDerivedGroupSize = storedGroupSize;
            for (int index = 1; index < gmc.length; index++) {
                int tid = gmc[index];
                if ((!Static.contains(tid, timeSeriesInAGap)) && Static.contains(tid, derivedTimeSeries)) {
                    storedAndDerivedGroupSize += 1;
                }
            }

            segments = new SegmentGroup[storedAndDerivedGroupSize];
            for (int index = 1; index < gmc.length; index++) {
                int tid = gmc[index];
                if (!Static.contains(tid, timeSeriesInAGap)) {
                    //Offsets store the following: [0] Group Offset, [1] Group Size, [2] Temporal Offset
                    byte[] offset = ByteBuffer.allocate(12).putInt(nextSegment + 1).putInt(storedGroupSize).putInt(temporalOffset).array();
                    segments[nextSegment] = new SegmentGroup(tid, this.startTime, this.samplingInterval, this.endTime, this.mtid, this.model, offset);
                    nextSegment++;
                }
            }
        }

        //The segment for a derived time series are the same as the segment of their source time series, only the tid is changed
        for (int i = 0, j = 0; i < derivedTimeSeries.length && j < segments.length; ) {
            if (derivedTimeSeries[i] == segments[j].gid) {
                segments[nextSegment] = new SegmentGroup(derivedTimeSeries[i + 1], this.startTime,this.samplingInterval, this.endTime,
                        this.mtid, this.model, segments[j].gaps);
                nextSegment++;
                i += 2;
            } else {
                j++;
            }
        }
        return segments;
    }

    public Segment[] toSegments(Storage storage) {
        int[][] groupMetadataCache = storage.groupMetadataCache();
        SegmentGroup[] sgs = this.explode(groupMetadataCache, storage.groupDerivedCache());
        Segment[] segments = new Segment[sgs.length];

        ModelType m = storage.modelTypeCache()[mtid];
        int[] gmc = groupMetadataCache[this.gid];
        for (int i = 0; i < sgs.length; i++) {
            segments[i] = m.get(sgs[i].gid, this.startTime, this.endTime, gmc[0], this.model, sgs[i].gaps);
        }
        return segments;
    }
}
