package dk.aau.modelardb.core.GroupBasedCompression;

import MockData.CSVTimeSeriesProviderHelper;
import MockData.ConfigurationProvider;
import MockData.MockSegmentFunction;
import dk.aau.modelardb.core.model.DataSlice;
import dk.aau.modelardb.core.model.compression.ModelType;
import dk.aau.modelardb.core.model.compression.ModelTypeFactory;
import dk.aau.modelardb.core.timeseries.TimeSeriesCSV;
import dk.aau.modelardb.core.utility.SegmentFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class SegmentGeneratorControllerTest {

    @BeforeAll
    static void init() {
        ConfigurationProvider.setDefaultValuesForConfigurationInstance();
    }

    @AfterAll
    static void cleanup(){
        ConfigurationProvider.removeDefaultValues();
    }

    private static TimeSeriesCSV createTimeSeriesN(int n) {
        String relativePath = "src/test/java/dk/aau/modelardb/core/GroupBasedCompression/SegmentGeneratorControllerTestData/";
        String path = relativePath + "time_series_" + n + ".csv";
        return CSVTimeSeriesProviderHelper.createTimeSeries(path, n);
    }

    TimeSeriesGroup createTimeSeriesGroup(List<Integer> timeSeriesNos) {

        int amtTimeSeries = timeSeriesNos.size();

        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[amtTimeSeries];
        int i = 0;
        for (Integer timeSeriesNo : timeSeriesNos) {
            tsArray[i] = createTimeSeriesN(timeSeriesNo);
            i++;
        }

        TimeSeriesGroup group = new TimeSeriesGroup(1, tsArray);
        group.initialize();
        return group;
    }

    private void printOutput(MockSegmentGeneratorSupplier segmentGeneratorSupplier) {
        for (MockSegmentGenerator createdSegmentGenerator : segmentGeneratorSupplier.createdSegmentGenerators) {
            var output = createdSegmentGenerator.getOutput();
            if(output.size() == 1) {
                System.out.println("SG: (SI:" + createdSegmentGenerator.si + " -> " + createdSegmentGenerator.tids + ") closed before receiving data.");
            } else {
                System.out.println("SG: (SI:" + createdSegmentGenerator.si + " -> " + createdSegmentGenerator.tids + ")'s data:");
                for (String s : output) {
                    System.out.println(s);
                }
            }
        }
    }

    private void assertOutputString(MockSegmentGeneratorSupplier segmentGeneratorSupplier, String expectedOutput) {
        StringBuilder actualOutput = new StringBuilder();

        for (MockSegmentGenerator createdSegmentGenerator : segmentGeneratorSupplier.createdSegmentGenerators) {
            var outputList  = createdSegmentGenerator.getOutput();
            if(outputList.size() == 1) {
                actualOutput.append("SG: (SI:").append(createdSegmentGenerator.si).append(" -> ").append(createdSegmentGenerator.tids).append(") closed before receiving data.");
                actualOutput.append("\n");
            } else {
                actualOutput.append("SG: (SI:").append(createdSegmentGenerator.si).append(" -> ").append(createdSegmentGenerator.tids).append(")'s data:");
                actualOutput.append("\n");
                for (String s : outputList) {
                    actualOutput.append(s);
                    actualOutput.append("\n");
                }
            }
        }
        var temp = actualOutput.toString();
        Assertions.assertEquals(expectedOutput, temp);
    }


    @Test
    void OneTimeSeriesWhereSIChanges() {
        List<Integer> timeSeriesNos = new ArrayList<>();
        timeSeriesNos.add(1);
        TimeSeriesGroup timeSeriesGroup = createTimeSeriesGroup(timeSeriesNos);
        MockSegmentGeneratorSupplier segmentGeneratorSupplier = new MockSegmentGeneratorSupplier(timeSeriesGroup);
        SegmentGeneratorController controller = new SegmentGeneratorController(timeSeriesGroup, segmentGeneratorSupplier);

        controller.start();

        //printOutput(segmentGeneratorSupplier);
        String expected =
                "SG: (SI:100 -> [1])'s data:\n" +
                "[ValueDataPoint: [tid: 1 | time: 100 | val: 1.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 1 | time: 200 | val: 1.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 1 | time: 300 | val: 1.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 1 | time: 400 | val: 1.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 1 | time: 500 | val: 1.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[1]\n" +
                "SG: (SI:200 -> [1])'s data:\n" +
                "[ValueDataPoint: [tid: 1 | time: 600 | val: 1.0 | si: 200]]\n" +
                "[ValueDataPoint: [tid: 1 | time: 800 | val: 1.0 | si: 200]]\n" +
                "[ValueDataPoint: [tid: 1 | time: 1000 | val: 1.0 | si: 200]]\n" +
                "Closed Segment Generator for tids:[1]\n";
        assertOutputString(segmentGeneratorSupplier, expected);
    }

    @Test
    void TwoTimeSerieOneStartsAtDifferentSIThenJoins() {
        List<Integer> timeSeriesNos = new ArrayList<>();
        timeSeriesNos.add(2);
        timeSeriesNos.add(3);

        TimeSeriesGroup timeSeriesGroup = createTimeSeriesGroup(timeSeriesNos);
        MockSegmentGeneratorSupplier segmentGeneratorSupplier = new MockSegmentGeneratorSupplier(timeSeriesGroup);
        SegmentGeneratorController controller = new SegmentGeneratorController(timeSeriesGroup, segmentGeneratorSupplier);

        controller.start();

        //printOutput(segmentGeneratorSupplier);
        String expected =
                "SG: (SI:100 -> [2]) closed before receiving data.\n" +
                "SG: (SI:100 -> [2, 3]) closed before receiving data.\n" +
                "SG: (SI:100 -> [2])'s data:\n" +
                "[ValueDataPoint: [tid: 2 | time: 100 | val: 2.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 2 | time: 200 | val: 2.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 2 | time: 300 | val: 2.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 2 | time: 400 | val: 2.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 2 | time: 500 | val: 2.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 2 | time: 600 | val: 2.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[2]\n" +
                "SG: (SI:200 -> [3])'s data:\n" +
                "[ValueDataPoint: [tid: 3 | time: 200 | val: 3.0 | si: 200]]\n" +
                "[ValueDataPoint: [tid: 3 | time: 400 | val: 3.0 | si: 200]]\n" +
                "[ValueDataPoint: [tid: 3 | time: 600 | val: 3.0 | si: 200]]\n" +
                "Closed Segment Generator for tids:[3]\n" +
                "SG: (SI:100 -> [2, 3])'s data:\n" +
                "[ValueDataPoint: [tid: 2 | time: 700 | val: 2.0 | si: 100], ValueDataPoint: [tid: 3 | time: 700 | val: 3.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 2 | time: 800 | val: 2.0 | si: 100], ValueDataPoint: [tid: 3 | time: 800 | val: 3.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 2 | time: 900 | val: 2.0 | si: 100], ValueDataPoint: [tid: 3 | time: 900 | val: 3.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 2 | time: 1000 | val: 2.0 | si: 100], ValueDataPoint: [tid: 3 | time: 1000 | val: 3.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[2, 3]\n";
        assertOutputString(segmentGeneratorSupplier, expected);
    }

    @Test
    void ThreeTimeSeriesSplitIntoThreeGenerators() {
        List<Integer> timeSeriesNos = new ArrayList<>();
        timeSeriesNos.add(4);
        timeSeriesNos.add(5);
        timeSeriesNos.add(6);

        TimeSeriesGroup timeSeriesGroup = createTimeSeriesGroup(timeSeriesNos);
        MockSegmentGeneratorSupplier segmentGeneratorSupplier = new MockSegmentGeneratorSupplier(timeSeriesGroup);
        SegmentGeneratorController controller = new SegmentGeneratorController(timeSeriesGroup, segmentGeneratorSupplier);

        controller.start();

        //printOutput(segmentGeneratorSupplier);
        String expected =
                "SG: (SI:100 -> [4]) closed before receiving data.\n" +
                "SG: (SI:100 -> [4, 5]) closed before receiving data.\n" +
                "SG: (SI:100 -> [4, 5, 6])'s data:\n" +
                "[ValueDataPoint: [tid: 4 | time: 100 | val: 4.0 | si: 100], ValueDataPoint: [tid: 5 | time: 100 | val: 5.0 | si: 100], ValueDataPoint: [tid: 6 | time: 100 | val: 6.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 4 | time: 200 | val: 4.0 | si: 100], ValueDataPoint: [tid: 5 | time: 200 | val: 5.0 | si: 100], ValueDataPoint: [tid: 6 | time: 200 | val: 6.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 4 | time: 300 | val: 4.0 | si: 100], ValueDataPoint: [tid: 5 | time: 300 | val: 5.0 | si: 100], ValueDataPoint: [tid: 6 | time: 300 | val: 6.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 4 | time: 400 | val: 4.0 | si: 100], ValueDataPoint: [tid: 5 | time: 400 | val: 5.0 | si: 100], ValueDataPoint: [tid: 6 | time: 400 | val: 6.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 4 | time: 500 | val: 4.0 | si: 100], ValueDataPoint: [tid: 5 | time: 500 | val: 5.0 | si: 100], ValueDataPoint: [tid: 6 | time: 500 | val: 6.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[4, 5, 6]\n" +
                "SG: (SI:100 -> [4, 6]) closed before receiving data.\n" +
                "SG: (SI:200 -> [5])'s data:\n" +
                "[ValueDataPoint: [tid: 5 | time: 600 | val: 5.0 | si: 200]]\n" +
                "[ValueDataPoint: [tid: 5 | time: 800 | val: 5.0 | si: 200]]\n" +
                "[ValueDataPoint: [tid: 5 | time: 1000 | val: 5.0 | si: 200]]\n" +
                "Closed Segment Generator for tids:[5]\n" +
                "SG: (SI:100 -> [4])'s data:\n" +
                "[ValueDataPoint: [tid: 4 | time: 600 | val: 4.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 4 | time: 700 | val: 4.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 4 | time: 800 | val: 4.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 4 | time: 900 | val: 4.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 4 | time: 1000 | val: 4.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[4]\n" +
                "SG: (SI:50 -> [6])'s data:\n" +
                "[ValueDataPoint: [tid: 6 | time: 550 | val: 6.0 | si: 50]]\n" +
                "[ValueDataPoint: [tid: 6 | time: 600 | val: 6.0 | si: 50]]\n" +
                "[ValueDataPoint: [tid: 6 | time: 650 | val: 6.0 | si: 50]]\n" +
                "[ValueDataPoint: [tid: 6 | time: 700 | val: 6.0 | si: 50]]\n" +
                "[ValueDataPoint: [tid: 6 | time: 750 | val: 6.0 | si: 50]]\n" +
                "[ValueDataPoint: [tid: 6 | time: 800 | val: 6.0 | si: 50]]\n" +
                "[ValueDataPoint: [tid: 6 | time: 850 | val: 6.0 | si: 50]]\n" +
                "[ValueDataPoint: [tid: 6 | time: 900 | val: 6.0 | si: 50]]\n" +
                "[ValueDataPoint: [tid: 6 | time: 950 | val: 6.0 | si: 50]]\n" +
                "[ValueDataPoint: [tid: 6 | time: 1000 | val: 6.0 | si: 50]]\n" +
                "Closed Segment Generator for tids:[6]\n";
        assertOutputString(segmentGeneratorSupplier, expected);
    }

    @Test
    void ThreeTimeSeriesChangeToSI200AtDifferentTimes() {
        List<Integer> timeSeriesNos = new ArrayList<>();
        timeSeriesNos.add(7);
        timeSeriesNos.add(8);
        timeSeriesNos.add(9);

        TimeSeriesGroup timeSeriesGroup = createTimeSeriesGroup(timeSeriesNos);
        MockSegmentGeneratorSupplier segmentGeneratorSupplier = new MockSegmentGeneratorSupplier(timeSeriesGroup);
        SegmentGeneratorController controller = new SegmentGeneratorController(timeSeriesGroup, segmentGeneratorSupplier);

        controller.start();

        //printOutput(segmentGeneratorSupplier);
        String expected =
                "SG: (SI:100 -> [7]) closed before receiving data.\n" +
                "SG: (SI:100 -> [7, 8]) closed before receiving data.\n" +
                "SG: (SI:100 -> [7, 8, 9])'s data:\n" +
                "[ValueDataPoint: [tid: 7 | time: 100 | val: 7.0 | si: 100], ValueDataPoint: [tid: 8 | time: 100 | val: 8.0 | si: 100], ValueDataPoint: [tid: 9 | time: 100 | val: 9.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 7 | time: 200 | val: 7.0 | si: 100], ValueDataPoint: [tid: 8 | time: 200 | val: 8.0 | si: 100], ValueDataPoint: [tid: 9 | time: 200 | val: 9.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[7, 8, 9]\n" +
                "SG: (SI:100 -> [8, 9])'s data:\n" +
                "[ValueDataPoint: [tid: 8 | time: 300 | val: 8.0 | si: 100], ValueDataPoint: [tid: 9 | time: 300 | val: 9.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 8 | time: 400 | val: 8.0 | si: 100], ValueDataPoint: [tid: 9 | time: 400 | val: 9.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 8 | time: 500 | val: 8.0 | si: 100], ValueDataPoint: [tid: 9 | time: 500 | val: 9.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[8, 9]\n" +
                "SG: (SI:200 -> [7])'s data:\n" +
                "[ValueDataPoint: [tid: 7 | time: 400 | val: 7.0 | si: 200]]\n" +
                "Closed Segment Generator for tids:[7]\n" +
                "SG: (SI:100 -> [9])'s data:\n" +
                "[ValueDataPoint: [tid: 9 | time: 600 | val: 9.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 9 | time: 700 | val: 9.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[9]\n" +
                "SG: (SI:200 -> [7, 8])'s data:\n" +
                "[ValueDataPoint: [tid: 7 | time: 600 | val: 7.0 | si: 200], ValueDataPoint: [tid: 8 | time: 600 | val: 8.0 | si: 200]]\n" +
                "Closed Segment Generator for tids:[7, 8]\n" +
                "SG: (SI:200 -> [7, 8, 9])'s data:\n" +
                "[ValueDataPoint: [tid: 7 | time: 800 | val: 7.0 | si: 200], ValueDataPoint: [tid: 8 | time: 800 | val: 8.0 | si: 200], ValueDataPoint: [tid: 9 | time: 800 | val: 9.0 | si: 200]]\n" +
                "[ValueDataPoint: [tid: 7 | time: 1000 | val: 7.0 | si: 200], ValueDataPoint: [tid: 8 | time: 1000 | val: 8.0 | si: 200], ValueDataPoint: [tid: 9 | time: 1000 | val: 9.0 | si: 200]]\n" +
                "Closed Segment Generator for tids:[7, 8, 9]\n";
        assertOutputString(segmentGeneratorSupplier, expected);
    }

    @Test
    void ThreeTimeSeriesOneSplitsThenJoinAgain() {
        List<Integer> timeSeriesNos = new ArrayList<>();
        timeSeriesNos.add(10);
        timeSeriesNos.add(11);
        timeSeriesNos.add(12);

        TimeSeriesGroup timeSeriesGroup = createTimeSeriesGroup(timeSeriesNos);
        MockSegmentGeneratorSupplier segmentGeneratorSupplier = new MockSegmentGeneratorSupplier(timeSeriesGroup);
        SegmentGeneratorController controller = new SegmentGeneratorController(timeSeriesGroup, segmentGeneratorSupplier);

        controller.start();

        //printOutput(segmentGeneratorSupplier);
        String expected =
                "SG: (SI:100 -> [10]) closed before receiving data.\n" +
                "SG: (SI:100 -> [10, 11]) closed before receiving data.\n" +
                "SG: (SI:100 -> [10, 11, 12])'s data:\n" +
                "[ValueDataPoint: [tid: 10 | time: 100 | val: 10.0 | si: 100], ValueDataPoint: [tid: 11 | time: 100 | val: 11.0 | si: 100], ValueDataPoint: [tid: 12 | time: 100 | val: 12.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 10 | time: 200 | val: 10.0 | si: 100], ValueDataPoint: [tid: 11 | time: 200 | val: 11.0 | si: 100], ValueDataPoint: [tid: 12 | time: 200 | val: 12.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[10, 11, 12]\n" +
                "SG: (SI:100 -> [10, 11])'s data:\n" +
                "[ValueDataPoint: [tid: 10 | time: 300 | val: 10.0 | si: 100], ValueDataPoint: [tid: 11 | time: 300 | val: 11.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 10 | time: 400 | val: 10.0 | si: 100], ValueDataPoint: [tid: 11 | time: 400 | val: 11.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 10 | time: 500 | val: 10.0 | si: 100], ValueDataPoint: [tid: 11 | time: 500 | val: 11.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 10 | time: 600 | val: 10.0 | si: 100], ValueDataPoint: [tid: 11 | time: 600 | val: 11.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 10 | time: 700 | val: 10.0 | si: 100], ValueDataPoint: [tid: 11 | time: 700 | val: 11.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 10 | time: 800 | val: 10.0 | si: 100], ValueDataPoint: [tid: 11 | time: 800 | val: 11.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[10, 11]\n" +
                "SG: (SI:200 -> [12])'s data:\n" +
                "[ValueDataPoint: [tid: 12 | time: 400 | val: 12.0 | si: 200]]\n" +
                "[ValueDataPoint: [tid: 12 | time: 600 | val: 12.0 | si: 200]]\n" +
                "[ValueDataPoint: [tid: 12 | time: 800 | val: 12.0 | si: 200]]\n" +
                "Closed Segment Generator for tids:[12]\n" +
                "SG: (SI:100 -> [10, 11, 12])'s data:\n" +
                "[ValueDataPoint: [tid: 10 | time: 900 | val: 10.0 | si: 100], ValueDataPoint: [tid: 11 | time: 900 | val: 11.0 | si: 100], ValueDataPoint: [tid: 12 | time: 900 | val: 12.0 | si: 100]]\n" +
                "[ValueDataPoint: [tid: 10 | time: 1000 | val: 10.0 | si: 100], ValueDataPoint: [tid: 11 | time: 1000 | val: 11.0 | si: 100], ValueDataPoint: [tid: 12 | time: 1000 | val: 12.0 | si: 100]]\n" +
                "Closed Segment Generator for tids:[10, 11, 12]\n";
        assertOutputString(segmentGeneratorSupplier, expected);
    }



    static class MockSegmentGenerator extends SegmentGenerator {
        private boolean isFinalized;
        private List<String> output;
        private List<Integer> tids;
        private int si;

        MockSegmentGenerator(List<Integer> tids, int si, Supplier<ModelType[]> modelTypeInitializer) {
            super(-1, -1, null, modelTypeInitializer, null, tids, -1, -1, null, null);
            this.isFinalized = false;
            this.output = new ArrayList<>();
            this.tids = tids;
            this.si = si;
        }

        @Override
        public boolean isFinalized() {
            return isFinalized;
        }

        @Override
        void close() {
            output.add("Closed Segment Generator for tids:" + tids.toString());
        }

        @Override
        public void consumeSlice(DataSlice slice) {
            output.add(slice.getValueDataPoints().toString());
        }

        public List<String> getOutput() {
            return output;
        }

    }

    static class MockSegmentGeneratorSupplier extends SegmentGeneratorSupplier {
        List<MockSegmentGenerator> createdSegmentGenerators;
        TimeSeriesGroup timeSeriesGroup;
        Supplier<ModelType[]> modelTypeInitializer;


        public MockSegmentGeneratorSupplier(TimeSeriesGroup timeSeriesGroup) {
            super(timeSeriesGroup, null, null, -1, null, null, -1);
            this.timeSeriesGroup = timeSeriesGroup;
            this.createdSegmentGenerators = new ArrayList<>();
            this.modelTypeInitializer = createModelTypeInitializer();
        }

        private Supplier<ModelType[]> createModelTypeInitializer() {
            float errorBound = 0.0F;
            int lengthBound = 3;
            String[] modelTypeNames = {"dk.aau.modelardb.core.model.compression.PMC_MeanModelType",
                    "dk.aau.modelardb.core.model.compression.SwingFilterModelType", "dk.aau.modelardb.core.model.compression.FacebookGorillaModelType"};
            int[] mtids = {2, 3, 4};

            return () -> ModelTypeFactory.getModelTypes(modelTypeNames, mtids, errorBound, lengthBound);
        }

        @Override
        public SegmentGenerator get(List<Integer> tids, int si) {
            var temp = new MockSegmentGenerator(tids, si, modelTypeInitializer);
            createdSegmentGenerators.add(temp);
            return temp;
        }
    }
}