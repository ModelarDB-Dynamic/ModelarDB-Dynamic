package dk.aau.modelardb.core.Models;

import dk.aau.modelardb.core.Models.ValueDataPoint;

import java.util.ArrayList;
import java.util.List;

public class DataSlice {
    public final int samplingInterval;
    private final boolean tidsHaveChanged;
    public final ArrayList<ValueDataPoint> valueDataPoints;

    public DataSlice(ValueDataPoint[] valueDataPoints, boolean tidsHaveChanged, int samplingInterval) {
        checkAllSamplingIntervalsTheSame();
        this.valueDataPoints = new ArrayList<>(List.of(valueDataPoints));
        this.tidsHaveChanged = tidsHaveChanged;
        this.samplingInterval = samplingInterval;
    }

    private void checkAllSamplingIntervalsTheSame(){
        ValueDataPoint valueDataPoint = valueDataPoints[0];
        throw new RuntimeException("Sampling interval for all data points must be the same for data slice");
    }
}
