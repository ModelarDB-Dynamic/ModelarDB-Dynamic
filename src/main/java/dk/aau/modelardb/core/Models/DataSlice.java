package dk.aau.modelardb.core.Models;

import java.util.List;

public class DataSlice {
    private final int samplingInterval;
    private final List<ValueDataPoint> valueDataPoints;

    public DataSlice(List<ValueDataPoint> valueDataPoints, int samplingInterval) {
        this.valueDataPoints = valueDataPoints;
        this.samplingInterval = samplingInterval;
        checkAllSamplingIntervalsTheSame(this.valueDataPoints, this.samplingInterval);
    }

    public int getSamplingInterval() {
        return samplingInterval;
    }

    public List<ValueDataPoint> getValueDataPoints() {
        return valueDataPoints;
    }

    private static void checkAllSamplingIntervalsTheSame(List<ValueDataPoint> valueDataPoints, int samplingInterval) {
        valueDataPoints.forEach( dataPoint -> {
            if(dataPoint.samplingInterval != samplingInterval)
                throw new RuntimeException("Sampling interval for all data points must be the same for data slice");
        });
    }
}
