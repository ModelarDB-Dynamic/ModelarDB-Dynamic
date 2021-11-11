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
                for (String s : createdSegmentGenerator.getOutput()) {
                    System.out.println(s);
                }
            }
        }
    }

    @Test
    void OneTimeSeriesWhereSIChanges() {
        List<Integer> timeSeriesNos = new ArrayList<>();
        timeSeriesNos.add(1);
        TimeSeriesGroup timeSeriesGroup = createTimeSeriesGroup(timeSeriesNos);
        MockSegmentGeneratorSupplier segmentGeneratorSupplier = new MockSegmentGeneratorSupplier(timeSeriesGroup);
        SegmentGeneratorController controller = new SegmentGeneratorController(timeSeriesGroup, segmentGeneratorSupplier);

        controller.start();

        printOutput(segmentGeneratorSupplier);
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

        printOutput(segmentGeneratorSupplier);
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

        printOutput(segmentGeneratorSupplier);
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