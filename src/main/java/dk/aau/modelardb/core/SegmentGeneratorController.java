package dk.aau.modelardb.core;

import java.util.HashMap;
import java.util.Map;

public class SegmentGeneratorController {
    private TimeSeriesGroup timeSeriesGroup;
    private Map<Integer, SegmentGenerator> samplingIntervalToSegmentGenerator;

    public SegmentGeneratorController(TimeSeriesGroup timeSeriesGroup) {
        this.timeSeriesGroup = timeSeriesGroup;
        this.samplingIntervalToSegmentGenerator = new HashMap<>();
    }
}
