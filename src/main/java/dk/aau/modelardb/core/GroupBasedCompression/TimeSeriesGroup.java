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

import dk.aau.modelardb.core.Models.DataSlice;
import dk.aau.modelardb.core.Models.ValueDataPoint;
import dk.aau.modelardb.core.timeseries.AsyncTimeSeries;
import dk.aau.modelardb.core.timeseries.TimeSeries;
import org.apache.commons.lang.NotImplementedException;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Selector;
import java.util.*;

public class TimeSeriesGroup implements Serializable {

    /**
     * Instance Variables
     **/
    public final int gid;
    public final boolean isAsync;
    public final int samplingInterval;
    private final TimeSeries[] timeSeries;
    private final Map<Integer, Integer> tidToTimeSeriesIndex;
    private int timeSeriesActive;
    private int timeSeriesHasNext;
    private PriorityQueue<ValueDataPoint> nextValueDataPointForEachTimeSeries = new PriorityQueue<>((dp1, dp2) -> {
        Comparator<ValueDataPoint> timestampComparator = Comparator.comparingLong(dp -> dp.timestamp);
        Comparator<ValueDataPoint> samplingIntervalComparator = Comparator.comparingInt(dp -> dp.samplingInterval);

        int comparedTimestamp = timestampComparator.compare(dp1, dp2);
        return comparedTimestamp == 0 ? comparedTimestamp : samplingIntervalComparator.compare(dp1, dp2);
    });

    /**
     * Constructors
     **/
    public TimeSeriesGroup(int gid, TimeSeries[] timeSeries) {
        if (timeSeries.length == 0) {
            throw new UnsupportedOperationException("CORE: a group must consist of at least one time series");
        }

        //Each time series is assumed to have the same boundness
        this.isAsync = timeSeries[0] instanceof AsyncTimeSeries;
        this.samplingInterval = timeSeries[0].getCurrentSamplingInterval();
        for (TimeSeries ts : timeSeries) {
            if (this.isAsync != ts instanceof AsyncTimeSeries) {
                throw new UnsupportedOperationException("CORE: All time series in a group must be bounded or unbounded");
            }
        }

        //Initializes variables for holding the latest data point for each time series
        this.gid = gid;
        this.timeSeries = timeSeries;
        this.tidToTimeSeriesIndex = new HashMap<>();
        for (int i = 0; i < timeSeries.length; i++) {
            this.tidToTimeSeriesIndex.put(timeSeries[i].tid,i);
        }
        this.timeSeriesHasNext = timeSeries.length;
    }

    /**
     * Public Methods
     **/
    public void initialize() {

        for (TimeSeries ts : this.timeSeries) {
            ts.open();

            // Initialize the first slice in P queue
            if (ts.hasNext()) {
                nextValueDataPointForEachTimeSeries.add(ts.next());
            }
        }
    }

    public void attachToSelector(Selector s, SegmentGenerator mg) throws IOException {
        for (TimeSeries ts : this.timeSeries) {
            if (ts instanceof AsyncTimeSeries) {
                ((AsyncTimeSeries) ts).attachToSelector(s, mg);
            }
        }
    }

    public TimeSeries[] getTimeSeries() {
        return this.timeSeries;
    }

    public String getTids() {
        StringJoiner sj = new StringJoiner(",", "{", "}");
        for (TimeSeries ts : this.timeSeries) {
            sj.add(Integer.toString(ts.tid));
        }
        return sj.toString();
    }

    public String getSources() {
        StringJoiner sj = new StringJoiner(",", "{", "}");
        for (TimeSeries ts : this.timeSeries) {
            sj.add(ts.source);
        }
        return sj.toString();
    }

    public int size() {
        return this.timeSeries.length;
    }

    public boolean hasNext() {
        return this.timeSeriesHasNext != 0;
    }

    public DataSlice GetSlice() {
        //Prepares the data points for the next SI
        List<ValueDataPoint> valueDataPointList = new ArrayList<>();
        do {
            ValueDataPoint point = this.nextValueDataPointForEachTimeSeries.poll(); //TODO #MarryJane esben can waste 5 hours renaming #GoodUseOfTime
            valueDataPointList.add(point);

            int timeSeriesIndex = this.tidToTimeSeriesIndex.get(point.getTid());
            ValueDataPoint nextPoint = this.timeSeries[timeSeriesIndex].next();

            if (nextPoint == null)
                throw new RuntimeException();
        } while (!nextValueDataPointForEachTimeSeries.isEmpty()
                && sameSIAndSameTimestamp(valueDataPointList.get(0), this.nextValueDataPointForEachTimeSeries.peek()));

        return new DataSlice(valueDataPointList, valueDataPointList.get(0).samplingInterval);
    }


    private boolean sameSIAndSameTimestamp(ValueDataPoint first, ValueDataPoint second){
       if (first.timestamp != second.timestamp)
           return false;
        return first.samplingInterval == second.samplingInterval;
    }

    public int getActiveTimeSeries() {
        return this.timeSeriesActive;
    }

    public void close() {
        for (TimeSeries ts : this.timeSeries) {
            ts.close();
        }
    }
}
