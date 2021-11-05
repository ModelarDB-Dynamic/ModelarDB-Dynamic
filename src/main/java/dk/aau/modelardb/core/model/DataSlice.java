package dk.aau.modelardb.core.model;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataSlice {
    private final int samplingInterval;
    private final List<ValueDataPoint> valueDataPoints;

    public DataSlice(List<ValueDataPoint> valueDataPoints, int samplingInterval) {
        this.valueDataPoints = valueDataPoints;
        this.samplingInterval = samplingInterval;
        checkAllSamplingIntervalsTheSame(this.valueDataPoints, this.samplingInterval);
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

    public Map<Set<Integer>, DataSlice> getSubDataSlice(Set<Set<Integer>> tidss) {
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

    @Override
    public String toString() {
        valueDataPoints.sort(Comparator.comparingInt(DataPoint::getTid));

        return "DataSlice{" +
                "samplingInterval=" + samplingInterval +
                ", valueDataPoints=" + valueDataPoints +
                '}';
    }
}
