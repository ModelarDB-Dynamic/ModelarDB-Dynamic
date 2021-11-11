package dk.aau.modelardb.core.GroupBasedCompression;

import dk.aau.modelardb.core.model.compression.ModelType;
import dk.aau.modelardb.core.model.DataSlice;
import dk.aau.modelardb.core.model.SIConfigurationDataPoint;
import dk.aau.modelardb.core.utility.SegmentFunction;

import java.util.*;
import java.util.function.Supplier;

public class SegmentGeneratorController {
    /**
     * Fields from WorkingSet used to instantiate SegmentGenerator
     */
    private final TimeSeriesGroup timeSeriesGroup;
    private final SegmentGeneratorSupplier segmentGeneratorSupplier;

    private final Map<Integer, SegmentGenerator> SIToSegmentGenerator;
    private final Map<Integer, Set<Integer>> SItoTids;

    public SegmentGeneratorController(TimeSeriesGroup timeSeriesGroup, SegmentGeneratorSupplier segmentGeneratorSupplier) {
        this.timeSeriesGroup = timeSeriesGroup;
        this.segmentGeneratorSupplier = segmentGeneratorSupplier;
        this.SIToSegmentGenerator = new HashMap<>();
        this.SItoTids = new HashMap<>();
    }

    public void start() {
        while (timeSeriesGroup.hasNext()) {
            List<SIConfigurationDataPoint> configurationDataPoints = timeSeriesGroup.getConfigurationDataPoints();

            if (configurationDataPoints.isEmpty()) {
                DataSlice slice = timeSeriesGroup.getSlice();
                delegateSliceToSegmentGenerators(slice);
            } else {
                handleConfigDataPoints(configurationDataPoints);
            }
        }
    }

    private SegmentGenerator createSegmentGenerator(List<Integer> tids, int samplingInterval) {
        return segmentGeneratorSupplier.get(tids, samplingInterval);
    }

    private void handleConfigDataPoints(List<SIConfigurationDataPoint> configurationDataPoints) {

        for (SIConfigurationDataPoint configDataPoint : configurationDataPoints) {
            int newSi = configDataPoint.getNewSamplingInterval();
            int prevSi = configDataPoint.getPreviousSamplingInterval();
            int tid = configDataPoint.getTid();

            if (newSi == prevSi) {
                break; // Do nothing
            }
            // Handle new SI
            addTidToSegmentGenerator(newSi, tid);

            // Handle previous SI
            if (configDataPoint.hasPreviousSamplingInterval()) {
                removeTidFromSegmentGenerator(prevSi, tid);
            }
        }
    }

    private void addTidToSegmentGenerator(int samplingInterval, int tid) {
        finalizeSegmentGeneratorForSI(samplingInterval);

        if (!this.SItoTids.containsKey(samplingInterval)) {
            this.SItoTids.put(samplingInterval, new HashSet<>());
        }
        Set<Integer> tids = this.SItoTids.get(samplingInterval);
        tids.add(tid);

        SIToSegmentGenerator.put(samplingInterval, createSegmentGenerator(new ArrayList<>(tids), samplingInterval));
    }

    private void removeTidFromSegmentGenerator(int samplingInterval, int tid){
        finalizeSegmentGeneratorForSI(samplingInterval);

        Set<Integer> tids = this.SItoTids.get(samplingInterval);
        tids.remove(tid);

        if (tids.size() > 0) { // Create a new segment generator if there still exists some time series with the SI
            SIToSegmentGenerator.put(samplingInterval, createSegmentGenerator(new ArrayList<>(tids), samplingInterval));
        } else {
            SItoTids.remove(samplingInterval);
        }
    }


    private void finalizeSegmentGeneratorForSI(int si) {
        if(SIToSegmentGenerator.containsKey(si)){
            var previousSegmentGenerator = SIToSegmentGenerator.get(si);
            previousSegmentGenerator.close();
            SIToSegmentGenerator.remove(si);
       }
       // If not segment generator exists for the SI then finalize nothing
    }


    private void delegateSliceToSegmentGenerators(DataSlice slice) {
        int samplingInterval = slice.getSamplingInterval();

        if (!SIToSegmentGenerator.containsKey(samplingInterval)){
            throw new IllegalArgumentException("No segment generator exists for SI: " + samplingInterval);
        }

        SegmentGenerator segmentGenerator = SIToSegmentGenerator.get(samplingInterval);
        segmentGenerator.consumeSlice(slice);
    }


}
