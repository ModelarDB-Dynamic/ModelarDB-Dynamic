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

        checkAllSamplingIntervalsTheSame(this.valueDataPoints, this.samplingInterval);
        checkAllTimestampsAreTheSame(this.valueDataPoints, this.timeStamp);
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

    private static void checkAllSamplingIntervalsTheSame(List<ValueDataPoint> valueDataPoints, int samplingInterval) {
        valueDataPoints.forEach( dataPoint -> {
            if(dataPoint.samplingInterval != samplingInterval)
                throw new RuntimeException("Sampling interval for all data points must be the same for data slice");
        });
    }

    private static void checkAllTimestampsAreTheSame(List<ValueDataPoint> valueDataPoints, long timeStamp) {
        valueDataPoints.forEach( dataPoint -> {
            if(dataPoint.timestamp != timeStamp)
                throw new RuntimeException("Time stamp for all data points must be the same for data slice");
        });
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
        Set<Integer> tempTids = new HashSet<>(allTids);
        Set<Integer> tidsInSlice = valueDataPoints.stream().map(DataPoint::getTid).collect(Collectors.toSet());
        tempTids.removeAll(tidsInSlice);

        for (Integer tid : tempTids) {
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
