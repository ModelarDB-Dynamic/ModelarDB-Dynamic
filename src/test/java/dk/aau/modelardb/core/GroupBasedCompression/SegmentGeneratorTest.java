package dk.aau.modelardb.core.GroupBasedCompression;

import MockData.CSVTimeSeriesProviderHelper;
import MockData.ConfigurationProvider;
import dk.aau.modelardb.core.Configuration;
import dk.aau.modelardb.core.model.compression.ModelType;
import dk.aau.modelardb.core.model.compression.ModelTypeFactory;
import dk.aau.modelardb.core.timeseries.TimeSeriesCSV;
import dk.aau.modelardb.core.utility.SegmentFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

class SegmentGeneratorTest {
    @BeforeAll
    static void init() {
        ConfigurationProvider.setDefaultValuesForConfigurationInstance();
    }

    private static TimeSeriesCSV createTimeSeriesN(int n) {
        int tid = n + 100;
        String relativePath = "src/test/java/dk/aau/modelardb/core/GroupBasedCompression/";
        String path = relativePath + "time_series_" + n + ".csv";
        return CSVTimeSeriesProviderHelper.createTimeSeries(path);
    }

    private SegmentGenerator createSegmentGenerator(TimeSeriesGroup group, SegmentFunction temporarySegmentStream, SegmentFunction finalizedSegmentStream, Set<Integer> permanentGapTids) {

        float errorBound = 0;
        int lengthBound = 50;
        Supplier<ModelType[]> modelTypeInitializer = createModelTypeInitializer(errorBound, lengthBound);
        ModelType fallbackModelType = ModelTypeFactory.getFallbackModelType(errorBound, lengthBound);

        int maximumLatency = 0; // Taken from IngestionTest.scala
        float dynamicSplitFraction = 1.0F / 10.0F; // Taken from IngestionTest.scala

        return new SegmentGenerator(group.gid, Configuration.INSTANCE.getSamplingInterval(), permanentGapTids,
                modelTypeInitializer, fallbackModelType, group.getTids(), maximumLatency, dynamicSplitFraction, temporarySegmentStream, finalizedSegmentStream);
    }

    private Supplier<ModelType[]> createModelTypeInitializer(float errorBound, int lengthBound) {
        String[] modelTypeNames = {"dk.aau.modelardb.core.model.compression.PMC_MeanModelType",
                "dk.aau.modelardb.core.model.compression.SwingFilterModelType", "dk.aau.modelardb.core.model.compression.FacebookGorillaModelType"};
        int[] mtids = {2, 3, 4};

        return () -> ModelTypeFactory.getModelTypes(modelTypeNames, mtids, errorBound, lengthBound);
    }

    static class MockSegmentFunction implements SegmentFunction {
        private final List<SegmentGroup> segments;

        public MockSegmentFunction() {
            this.segments = new ArrayList<>();
        }

        @Override
        public void emit(int gid, long startTime, int samplingInterval, long endTime, int mtid, byte[] model, byte[] gaps) {
            SegmentGroup segmentGroup = new SegmentGroup(gid, startTime, samplingInterval, endTime, mtid, model, gaps);
            segments.add(segmentGroup);
        }

        public List<SegmentGroup> getSegments() {
            return segments;
        }
    }

    private void printAllSegments(List<SegmentGroup> segments) {

        for (SegmentGroup segment : segments) {
            System.out.println(segment);
        }
    }

    @Test
    void consumeSlice() {
        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[1];
        tsArray[0] = createTimeSeriesN(1);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();


        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();
        permanentGaps.add(10);

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);

        while (group.hasNext()) {
            segmentGenerator.consumeSlice(group.getSlice());
        }
        segmentGenerator.close();


        printAllSegments(finalizedSegmentStream.getSegments());

    }
}
