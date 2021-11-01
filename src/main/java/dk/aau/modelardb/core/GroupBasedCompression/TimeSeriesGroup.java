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

import dk.aau.modelardb.core.Models.DataPoint;
import dk.aau.modelardb.core.Models.DataSlice;
import dk.aau.modelardb.core.Models.SIConfigurationDataPoint;
import dk.aau.modelardb.core.Models.ValueDataPoint;
import dk.aau.modelardb.core.timeseries.AsyncTimeSeries;
import dk.aau.modelardb.core.timeseries.TimeSeries;

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
    private Map<Integer, Integer> tidToTimeSeriesIndex;
    private int amountOfTimeSeriesWithNext;
    private PriorityQueue<ValueDataPoint> nextValueDataPointPriorityQueue = new PriorityQueue<>(getDataPointComparator());
    private List<SIConfigurationDataPoint> configurationDataPoints;

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

        this.configurationDataPoints = new ArrayList<>();
        tidToTimeSeriesIndex = new HashMap<>();
        for (int i = 0; i < timeSeries.length; i++) {
            tidToTimeSeriesIndex.put(timeSeries[i].tid, i);
        }

        //Initializes variables for holding the latest data point for each time series
        this.gid = gid;
        this.timeSeries = timeSeries;
        this.amountOfTimeSeriesWithNext = timeSeries.length;
    }

    private static Comparator<ValueDataPoint> getDataPointComparator() {
        return (ValueDataPoint dp1, ValueDataPoint dp2) -> {
            Comparator<ValueDataPoint> timestampComparator = Comparator.comparingLong(dp -> dp.timestamp);
            Comparator<ValueDataPoint> samplingIntervalComparator = Comparator.comparingInt(dp -> dp.samplingInterval);

            int comparedTimestamp = timestampComparator.compare(dp1, dp2);
            return comparedTimestamp == 0 ? comparedTimestamp : samplingIntervalComparator.compare(dp1, dp2);
        };
    }

    /**
     * Public Methods
     **/
    public void initialize() {
        for (TimeSeries ts : this.timeSeries) {
            ts.open();

            // Initialize the first slice in P queue
            DataPoint next = ts.next();
            while (next.isConfigurationDataPoint()) {
                this.configurationDataPoints.add((SIConfigurationDataPoint) next);
                next = ts.next();
            }
            nextValueDataPointPriorityQueue.add((ValueDataPoint) next);
        }
    }

/*    public void attachToSelector(Selector s, SegmentGenerator mg) throws IOException {
        for (TimeSeries ts : this.timeSeries) {
            if (ts instanceof AsyncTimeSeries) {
                ((AsyncTimeSeries) ts).attachToSelector(s, mg);
            }
        }
    }*/

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
        return this.amountOfTimeSeriesWithNext != 0;
    }

    public DataSlice getSlice() {
        //Prepares the data points for the next SI
        List<ValueDataPoint> valueDataPointList = new ArrayList<>();
        do {
            ValueDataPoint point = this.nextValueDataPointPriorityQueue.poll();
            valueDataPointList.add(point);

            ValueDataPoint nextValueDatapoint = nextValueDatapoint(point.getTid());
            this.nextValueDataPointPriorityQueue.add(nextValueDatapoint);

        } while (!nextValueDataPointPriorityQueue.isEmpty()
                && sameSIAndSameTimestamp(valueDataPointList.get(0), this.nextValueDataPointPriorityQueue.peek()));


        return new DataSlice(valueDataPointList, valueDataPointList.get(0).samplingInterval);
    }

    private ValueDataPoint nextValueDatapoint(int tid) {
        int timeSeriesIndex = this.tidToTimeSeriesIndex.get(tid);
        TimeSeries currentTimeSeries = this.timeSeries[timeSeriesIndex];

        while(currentTimeSeries.hasNext()) {
            DataPoint next = currentTimeSeries.next();
            if (next.isConfigurationDataPoint()) {
                this.configurationDataPoints.add((SIConfigurationDataPoint) next);
            } else {
                return (ValueDataPoint) next;
            }
        }
        throw new RuntimeException("Last point of timeseries {" + tid + "} is config datapoint");
    }


    private boolean sameSIAndSameTimestamp(ValueDataPoint first, ValueDataPoint second) {
        if (first.timestamp != second.timestamp)
            return false;
        return first.samplingInterval == second.samplingInterval;
    }

    public int getActiveTimeSeries() {
        return this.amountOfTimeSeriesWithNext;
    }

    public void close() {
        for (TimeSeries ts : this.timeSeries) {
            ts.close();
        }
    }

    public List<SIConfigurationDataPoint> getConfigurationDataPoints() {
        ArrayList<SIConfigurationDataPoint> result = new ArrayList<>(this.configurationDataPoints);
        this.configurationDataPoints.clear();
        return result;
    }
}
