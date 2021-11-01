package dk.aau.modelardb.core.Models;

import dk.aau.modelardb.core.Models.ValueDataPoint;

import java.util.ArrayList;
import java.util.List;

public class DataSlice {
    public final int samplingInterval;
    public final List<ValueDataPoint> valueDataPoints;

    public DataSlice(List<ValueDataPoint> valueDataPoints,  int samplingInterval) {
        checkAllSamplingIntervalsTheSame();
        this.valueDataPoints = valueDataPoints;
        this.samplingInterval = samplingInterval;
    }

    private void checkAllSamplingIntervalsTheSame(){
        ValueDataPoint valueDataPoint = valueDataPoints[0];
        throw new RuntimeException("Sampling interval for all data points must be the same for data slice");
    }
}
