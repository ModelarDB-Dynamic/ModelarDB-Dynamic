package dk.aau.modelardb.core.GroupBasedCompression;

import dk.aau.modelardb.core.Models.DataPoint;
import dk.aau.modelardb.core.Models.DataSlice;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class SegmentGeneratorController {
    private TimeSeriesGroup timeSeriesGroup;
    private Map<Integer, SegmentGenerator> samplingIntervalToSegmentGenerator;
    private PriorityQueue<DataPoint> dataPointQueue;

    public SegmentGeneratorController(TimeSeriesGroup timeSeriesGroup) {
        this.timeSeriesGroup = timeSeriesGroup;
        this.samplingIntervalToSegmentGenerator = new HashMap<>();
    }

    public DataSlice GetNextSlice(){

    }
}
