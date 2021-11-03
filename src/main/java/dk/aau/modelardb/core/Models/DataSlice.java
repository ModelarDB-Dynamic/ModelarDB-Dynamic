package dk.aau.modelardb.core.Models;

import scala.tools.nsc.doc.model.Val;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
}
