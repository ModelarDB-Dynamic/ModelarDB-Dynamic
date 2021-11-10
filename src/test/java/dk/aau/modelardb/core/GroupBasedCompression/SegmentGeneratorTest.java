package dk.aau.modelardb.core.GroupBasedCompression;

import MockData.CSVTimeSeriesProviderHelper;
import MockData.ConfigurationProvider;
import dk.aau.modelardb.core.Configuration;
import dk.aau.modelardb.core.model.compression.ModelType;
import dk.aau.modelardb.core.model.compression.ModelTypeFactory;
import dk.aau.modelardb.core.timeseries.TimeSeriesCSV;
import dk.aau.modelardb.core.utility.SegmentFunction;
import org.junit.jupiter.api.*;

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

    @AfterAll
    static void cleanup(){
        ConfigurationProvider.removeDefaultValues();
    }

    private static TimeSeriesCSV createTimeSeriesN(int n) {
        String relativePath = "src/test/java/dk/aau/modelardb/core/GroupBasedCompression/SegmentGeneratorTestData/";
        String path = relativePath + "time_series_" + n + ".csv";
        return CSVTimeSeriesProviderHelper.createTimeSeries(path, n);
    }

    private SegmentGenerator createSegmentGenerator(TimeSeriesGroup group, SegmentFunction temporarySegmentStream, SegmentFunction finalizedSegmentStream, Set<Integer> permanentGapTids) {
        float errorBound = 0;
        int lengthBound = 3; // Important for GORILLA AND HOW MANY POINTS WE ADD before the actual split
        Supplier<ModelType[]> modelTypeInitializer = createModelTypeInitializer(errorBound, lengthBound);
        ModelType fallbackModelType = ModelTypeFactory.getFallbackModelType(errorBound, lengthBound);

        int maximumLatency = 0; // Taken from IngestionTest.scala
        float dynamicSplitFraction = 1.0F; // Taken from IngestionTest.scala

        return new SegmentGenerator(group.gid, Configuration.INSTANCE.getSamplingInterval(), permanentGapTids,
                modelTypeInitializer, fallbackModelType, new ArrayList<>(group.getTids()), maximumLatency, dynamicSplitFraction, temporarySegmentStream, finalizedSegmentStream);
    }

    // We dont allow usage of GORILLA to simplify tests
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

    private void consumeAllSlicesFromGroup(TimeSeriesGroup group, SegmentGenerator segmentGenerator) {
        while (group.hasNext()) {
            segmentGenerator.consumeSlice(group.getSlice());
        }
        segmentGenerator.close();
    }

    private void printAllSegments(List<SegmentGroup> segments) {
        for (SegmentGroup segment : segments) {
            System.out.println(segment);
        }
    }

    private void assertOutputString(List<SegmentGroup> segments, String expectedOutput) {
        StringBuilder actualOutput = new StringBuilder();

        for (SegmentGroup segment : segments) {
            actualOutput.append(segment.toString());
            actualOutput.append('\n');
        }
        var temp = actualOutput.toString();
        Assertions.assertEquals(expectedOutput, temp);
    }

    @Test
    void OneTimeSeriesConstantModel() {
        int timeSeriesNo = 1;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[1];
        tsArray[0] = createTimeSeriesN(timeSeriesNo);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        // printAllSegments(finalizedSegmentStream.getSegments());
        String expected = "Segment: [gid: 1 | start: 100 | end: 1500| si: 100 | mtid: 2]\n";
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }

    @Test
    void OneTimeSeriesLinearModel() {
        int timeSeriesNo = 2;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[1];
        tsArray[0] = createTimeSeriesN(timeSeriesNo);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        //printAllSegments(finalizedSegmentStream.getSegments());
        String expected = "Segment: [gid: 1 | start: 100 | end: 1000| si: 100 | mtid: 3]\n";
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }

    @Test
    void OneTimeSeriesWithGaps() {
        int timeSeriesNo = 3;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[1];
        tsArray[0] = createTimeSeriesN(timeSeriesNo);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        // printAllSegments(finalizedSegmentStream.getSegments());
        String expected =
                "Segment: [gid: 1 | start: 100 | end: 500| si: 100 | mtid: 2]\n" +
                "Segment: [gid: 1 | start: 1100 | end: 1500| si: 100 | mtid: 2]\n";
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }

    @Test
    void TwoTimeSeriesOneEndsEarly() {
        int timeSeriesNoA = 1;
        int timeSeriesNoB = 4;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(timeSeriesNoA);
        tsArray[1] = createTimeSeriesN(timeSeriesNoB);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        String expected =
                "Segment: [gid: 1 | start: 100 | end: 1000| si: 100 | mtid: 2]\n" +
                        "Segment: [gid: 1 | start: 1100 | end: 1500| si: 100 | mtid: 2 | gaps: [4]]\n";
        //printAllSegments(finalizedSegmentStream.getSegments());
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }

    @Test
    void SplitTwoTimeSeries() {
        int timeSeriesNoA = 1;
        int timeSeriesNoB = 5;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(timeSeriesNoA);
        tsArray[1] = createTimeSeriesN(timeSeriesNoB);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        String expected =
                "Segment: [gid: 1 | start: 100 | end: 500| si: 100 | mtid: 2]\n" +
                "Segment: [gid: 1 | start: 600 | end: 800| si: 100 | mtid: 4]\n" +
                "Segment: [gid: 1 | start: 900 | end: 1500| si: 100 | mtid: 2 | gaps: [5]]\n" +
                "Segment: [gid: 1 | start: 900 | end: 1500| si: 100 | mtid: 2 | gaps: [1]]\n";

        // printAllSegments(finalizedSegmentStream.getSegments());
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }

    @Test
    void JoinTwoTimeSeries() {
        int timeSeriesNoA = 1;
        int timeSeriesNoB = 6;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(timeSeriesNoA);
        tsArray[1] = createTimeSeriesN(timeSeriesNoB);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        String expected =
                "Segment: [gid: 1 | start: 100 | end: 500| si: 100 | mtid: 2]\n" +
                "Segment: [gid: 1 | start: 600 | end: 800| si: 100 | mtid: 4]\n" +
                "Segment: [gid: 1 | start: 900 | end: 1100| si: 100 | mtid: 2 | gaps: [1]]\n" +
                "Segment: [gid: 1 | start: 900 | end: 1200| si: 100 | mtid: 2 | gaps: [6]]\n" +
                "Segment: [gid: 1 | start: 1200 | end: 1200| si: 100 | mtid: 4 | gaps: [1]]\n" +
                "Segment: [gid: 1 | start: 1300 | end: 1500| si: 100 | mtid: 2]\n";
        //printAllSegments(finalizedSegmentStream.getSegments());
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }

    @Test
    void GapInTwoSplitTimeSeries() {
        int timeSeriesNoA = 1;
        int timeSeriesNoB = 7;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(timeSeriesNoA);
        tsArray[1] = createTimeSeriesN(timeSeriesNoB);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        String expected =
                "Segment: [gid: 1 | start: 100 | end: 500| si: 100 | mtid: 2]\n" +
                "Segment: [gid: 1 | start: 600 | end: 800| si: 100 | mtid: 4]\n" +
                "Segment: [gid: 1 | start: 900 | end: 1200| si: 100 | mtid: 2 | gaps: [1]]\n" +
                "Segment: [gid: 1 | start: 900 | end: 1500| si: 100 | mtid: 2 | gaps: [7]]\n" +
                "Segment: [gid: 1 | start: 1400 | end: 1500| si: 100 | mtid: 2 | gaps: [1]]\n";
        //printAllSegments(finalizedSegmentStream.getSegments());
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }


    // We can't join them if gaps occur when trying to join them
    @Test
    void TwoTimeSeriesGapWhenTryingToJoin() {
        int timeSeriesNoA = 1;
        int timeSeriesNoB = 8;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(timeSeriesNoA);
        tsArray[1] = createTimeSeriesN(timeSeriesNoB);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        String expected =
               "Segment: [gid: 1 | start: 100 | end: 500| si: 100 | mtid: 2]\n" +
               "Segment: [gid: 1 | start: 600 | end: 800| si: 100 | mtid: 4]\n" +
               "Segment: [gid: 1 | start: 900 | end: 1100| si: 100 | mtid: 2 | gaps: [1]]\n" +
               "Segment: [gid: 1 | start: 900 | end: 1500| si: 100 | mtid: 2 | gaps: [8]]\n" +
               "Segment: [gid: 1 | start: 1400 | end: 1500| si: 100 | mtid: 2 | gaps: [1]]\n";
        //printAllSegments(finalizedSegmentStream.getSegments());
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }

    @Test
    void ThreeTimeSeriesTwoSplitAway() {
        int timeSeriesNoA = 10;
        int timeSeriesNoB = 11;
        int timeSeriesNoC = 12;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[3];
        tsArray[0] = createTimeSeriesN(timeSeriesNoA);
        tsArray[1] = createTimeSeriesN(timeSeriesNoB);
        tsArray[2] = createTimeSeriesN(timeSeriesNoC);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        String expected =
                "Segment: [gid: 1 | start: 100 | end: 500| si: 100 | mtid: 2]\n" +
                "Segment: [gid: 1 | start: 600 | end: 800| si: 100 | mtid: 4]\n" +
                "Segment: [gid: 1 | start: 900 | end: 2000| si: 100 | mtid: 2 | gaps: [11, 12]]\n" +
                "Segment: [gid: 1 | start: 900 | end: 2000| si: 100 | mtid: 2 | gaps: [10]]\n";

        //printAllSegments(finalizedSegmentStream.getSegments());
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }


    // 11 and 13 are not joined in the end because none of them emit any segment allowing the join to start
    @Test
    void ThreeTimeSeriesSplitAwayAtDifferentTimes() {
        int timeSeriesNoA = 10;
        int timeSeriesNoB = 11;
        int timeSeriesNoC = 13;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[3];
        tsArray[0] = createTimeSeriesN(timeSeriesNoA);
        tsArray[1] = createTimeSeriesN(timeSeriesNoB);
        tsArray[2] = createTimeSeriesN(timeSeriesNoC);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        String expected =
                "Segment: [gid: 1 | start: 100 | end: 500| si: 100 | mtid: 2]\n" +
                "Segment: [gid: 1 | start: 600 | end: 800| si: 100 | mtid: 4]\n" +
                "Segment: [gid: 1 | start: 900 | end: 1100| si: 100 | mtid: 2 | gaps: [11]]\n" +
                "Segment: [gid: 1 | start: 1200 | end: 1400| si: 100 | mtid: 4 | gaps: [11]]\n" +
                "Segment: [gid: 1 | start: 1500 | end: 2000| si: 100 | mtid: 2 | gaps: [11, 13]]\n" +
                "Segment: [gid: 1 | start: 900 | end: 2000| si: 100 | mtid: 2 | gaps: [10, 13]]\n" +
                "Segment: [gid: 1 | start: 1500 | end: 2000| si: 100 | mtid: 2 | gaps: [10, 11]]\n";

        //printAllSegments(finalizedSegmentStream.getSegments());
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }

    @Test
    void ThreeTimeSeriesSplitInto3Generators() {
        int timeSeriesNoA = 10;
        int timeSeriesNoB = 11;
        int timeSeriesNoC = 14;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[3];
        tsArray[0] = createTimeSeriesN(timeSeriesNoA);
        tsArray[1] = createTimeSeriesN(timeSeriesNoB);
        tsArray[2] = createTimeSeriesN(timeSeriesNoC);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        String expected =
                "Segment: [gid: 1 | start: 100 | end: 500| si: 100 | mtid: 2]\n" +
                "Segment: [gid: 1 | start: 600 | end: 800| si: 100 | mtid: 4]\n" +
                "Segment: [gid: 1 | start: 900 | end: 2000| si: 100 | mtid: 2 | gaps: [11, 14]]\n" +
                "Segment: [gid: 1 | start: 900 | end: 2000| si: 100 | mtid: 2 | gaps: [10, 14]]\n" +
                "Segment: [gid: 1 | start: 900 | end: 2000| si: 100 | mtid: 2 | gaps: [10, 11]]\n";

        // printAllSegments(finalizedSegmentStream.getSegments());
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }

    // If a time series has a gap at the split then it is split into its own segment generator
    @Test
    void ThreeTimeSeriesOneHasGapsAtSplit() {
        int timeSeriesNoA = 10;
        int timeSeriesNoB = 11;
        int timeSeriesNoC = 15;

        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[3];
        tsArray[0] = createTimeSeriesN(timeSeriesNoA);
        tsArray[1] = createTimeSeriesN(timeSeriesNoB);
        tsArray[2] = createTimeSeriesN(timeSeriesNoC);
        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        MockSegmentFunction temporarySegmentStream = new MockSegmentFunction();
        MockSegmentFunction finalizedSegmentStream = new MockSegmentFunction();
        Set<Integer> permanentGaps = new HashSet<>();

        SegmentGenerator segmentGenerator = createSegmentGenerator(group, temporarySegmentStream, finalizedSegmentStream, permanentGaps);
        consumeAllSlicesFromGroup(group, segmentGenerator);

        String expected =
                "Segment: [gid: 1 | start: 100 | end: 500| si: 100 | mtid: 2]\n" +
                "Segment: [gid: 1 | start: 600 | end: 800| si: 100 | mtid: 4 | gaps: [15]]\n" +
                "Segment: [gid: 1 | start: 900 | end: 2000| si: 100 | mtid: 2 | gaps: [11, 15]]\n" +
                "Segment: [gid: 1 | start: 900 | end: 2000| si: 100 | mtid: 2 | gaps: [10, 15]]\n" +
                "Segment: [gid: 1 | start: 1100 | end: 2000| si: 100 | mtid: 2 | gaps: [10, 11]]\n";
        // printAllSegments(finalizedSegmentStream.getSegments());
        assertOutputString(finalizedSegmentStream.getSegments(), expected);
    }
}
