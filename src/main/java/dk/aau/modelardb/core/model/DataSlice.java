/* Copyright 2021 The ModelarDB-Dynamic Contributors
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

package dk.aau.modelardb.core.model;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataSlice {
    private final int samplingInterval;
    private long timeStamp;
    private final List<ValueDataPoint> valueDataPoints;

    public DataSlice(List<ValueDataPoint> valueDataPoints, int samplingInterval) {
        this.valueDataPoints = valueDataPoints;
        this.samplingInterval = samplingInterval;
        this.timeStamp = valueDataPoints.get(0).timestamp;
    }

    private DataSlice(int samplingInterval) {
        this.samplingInterval = samplingInterval;
        this.valueDataPoints = new ArrayList<>();
    }

    public int getSamplingInterval() {
        return samplingInterval;
    }

    public List<ValueDataPoint> getValueDataPoints() {
        Collections.sort(this.valueDataPoints);
        return valueDataPoints;
    }

    public ValueDataPoint[] getDataPoints(){
        Collections.sort(this.valueDataPoints);
        return this.valueDataPoints.toArray(new ValueDataPoint[0]);
    }

    public Map<Set<Integer>, DataSlice> getSubDataSlices(Set<Set<Integer>> tidss) {
        Map<Set<Integer>, DataSlice> tidsToSubDataSlice = tidss.stream().collect(Collectors.toMap(Function.identity(), item -> new DataSlice(this.samplingInterval)));

        Map<Integer, Set<Integer>> tidToSetOfTids = new HashMap<>();
        for (Set<Integer> tids : tidss) {
            for (Integer  tid: tids) {
                tidToSetOfTids.put(tid, tids);
            }
        }

        for (ValueDataPoint valueDataPoint : this.valueDataPoints) {
            Set<Integer> tids = tidToSetOfTids.get(valueDataPoint.getTid());
            tidsToSubDataSlice.get(tids).valueDataPoints.add(valueDataPoint);
        }

     return tidsToSubDataSlice;
    }

    public void addGapsForTidsWithMissingPoints(Set<Integer> allTids) {
        Set<Integer> tidsNotInSlice = new HashSet<>(allTids);
        Set<Integer> tidsInSlice = valueDataPoints.stream().map(DataPoint::getTid).collect(Collectors.toSet());
        tidsNotInSlice.removeAll(tidsInSlice);

        for (Integer tid : tidsNotInSlice) {
            this.addGapPointForTid(tid);
        }
    }

    private void addGapPointForTid(int tid) {
        this.valueDataPoints.add(new ValueDataPoint(tid, this.timeStamp, Float.NaN, this.samplingInterval));
    }

    @Override
    public String toString() {
        valueDataPoints.sort(Comparator.comparingInt(DataPoint::getTid));

        return "DataSlice{" +
                "samplingInterval=" + samplingInterval +
                ", valueDataPoints=" + valueDataPoints +
                '}';
    }
}
