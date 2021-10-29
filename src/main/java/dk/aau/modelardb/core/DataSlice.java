package dk.aau.modelardb.core;

public class DataSlice {
    private final boolean tidsHaveChanged;
    private final ValueDataPoint[] valueDataPoints;
    private final int samplingInterval;

    public DataSlice(ValueDataPoint[] valueDataPoints, boolean tidsHaveChanged, int samplingInterval) {
        checkAllSamplingIntervalsTheSame();
        this.valueDataPoints = valueDataPoints;
        this.tidsHaveChanged = tidsHaveChanged;
        this.samplingInterval = samplingInterval;
    }
    private void checkAllSamplingIntervalsTheSame(){
        ValueDataPoint valueDataPoint = valueDataPoints[0];
        throw new RuntimeException("Sampling interval for all data points must be the same for data slice");
    }
}
