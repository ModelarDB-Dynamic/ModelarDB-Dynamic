package dk.aau.modelardb.core.GroupBasedCompression;

import MockData.CSVTimeSeriesProviderHelper;
import MockData.ConfigurationProvider;
import dk.aau.modelardb.core.model.DataSlice;
import dk.aau.modelardb.core.timeseries.TimeSeriesCSV;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TimeSeriesGroupTest {

    @BeforeAll
    static void init() {
        ConfigurationProvider.setDefaultValuesForConfigurationInstance();
    }

    @AfterAll
    static void cleanup(){
        ConfigurationProvider.removeDefaultValues();
    }

    private static TimeSeriesCSV createTimeSeriesN(int n) {
        String relativePath = "src/test/java/dk/aau/modelardb/core/GroupBasedCompression/TimeSeriesGroupTestData/";
        String path = relativePath + "time_series_" + n + ".csv";
        return CSVTimeSeriesProviderHelper.createTimeSeries(path, n);
    }

    private void printAllSlices(TimeSeriesGroup group) {
        while (group.hasNext()) {
            DataSlice slice = group.getSlice();
            System.out.println(slice);
        }
    }

    private void assertOutputString(TimeSeriesGroup group, String expectedOutput) {
        StringBuilder actualOutput = new StringBuilder();

        while (group.hasNext()) {
            actualOutput.append(group.getSlice().toString());
            actualOutput.append('\n');
        }
        var temp = actualOutput.toString();
        Assertions.assertEquals(expectedOutput, temp);
    }

    @Test
    void getSlice() {
        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(1);
        tsArray[1] = createTimeSeriesN(2);

        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        String expectedOutput = "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 100 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 100 | val: 2.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 200 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 200 | val: 2.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 300 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 300 | val: 2.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 400 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 400 | val: 2.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 500 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 500 | val: 2.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 600 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 600 | val: NaN | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 700 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 700 | val: NaN | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 800 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 800 | val: NaN | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 900 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 900 | val: 2.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 1000 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 1000 | val: 2.0 | si: 100]]}\n";
        assertOutputString(group, expectedOutput);
    }

    @Test
    void getSliceOneStartsLater() {
        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(1);
        tsArray[1] = createTimeSeriesN(3);

        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        String expectedOutput =
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 100 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 200 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 300 | val: 1.0 | si: 100], ValueDataPoint: [tid: 3 | time: 300 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 400 | val: 1.0 | si: 100], ValueDataPoint: [tid: 3 | time: 400 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 500 | val: 1.0 | si: 100], ValueDataPoint: [tid: 3 | time: 500 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 600 | val: 1.0 | si: 100], ValueDataPoint: [tid: 3 | time: 600 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 700 | val: 1.0 | si: 100], ValueDataPoint: [tid: 3 | time: 700 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 800 | val: 1.0 | si: 100], ValueDataPoint: [tid: 3 | time: 800 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 900 | val: 1.0 | si: 100], ValueDataPoint: [tid: 3 | time: 900 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 1000 | val: 1.0 | si: 100], ValueDataPoint: [tid: 3 | time: 1000 | val: 3.0 | si: 100]]}\n";
        assertOutputString(group, expectedOutput);
    }

    @Test
    void getSliceOneEndsEarly() {
        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(1);
        tsArray[1] = createTimeSeriesN(4);

        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        String expected = "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 100 | val: 1.0 | si: 100], ValueDataPoint: [tid: 4 | time: 100 | val: 4.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 200 | val: 1.0 | si: 100], ValueDataPoint: [tid: 4 | time: 200 | val: 4.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 300 | val: 1.0 | si: 100], ValueDataPoint: [tid: 4 | time: 300 | val: 4.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 400 | val: 1.0 | si: 100], ValueDataPoint: [tid: 4 | time: 400 | val: 4.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 500 | val: 1.0 | si: 100], ValueDataPoint: [tid: 4 | time: 500 | val: 4.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 600 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 700 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 800 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 900 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 1000 | val: 1.0 | si: 100]]}\n";
        assertOutputString(group, expected);
    }

    @Test
    void configPointInStart() {
        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(1);
        tsArray[1] = createTimeSeriesN(5);

        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        String expected =
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 100 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 200 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=200, valueDataPoints=[ValueDataPoint: [tid: 5 | time: 200 | val: 5.0 | si: 200]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 300 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 400 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=200, valueDataPoints=[ValueDataPoint: [tid: 5 | time: 400 | val: 5.0 | si: 200]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 500 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 600 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=200, valueDataPoints=[ValueDataPoint: [tid: 5 | time: 600 | val: 5.0 | si: 200]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 700 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 800 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=200, valueDataPoints=[ValueDataPoint: [tid: 5 | time: 800 | val: 5.0 | si: 200]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 900 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 1000 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=200, valueDataPoints=[ValueDataPoint: [tid: 5 | time: 1000 | val: 5.0 | si: 200]]}\n";

        assertOutputString(group, expected);
    }

    @Test
    void configInMiddle() {
        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(1);
        tsArray[1] = createTimeSeriesN(6);

        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        String expected =
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 100 | val: 1.0 | si: 100], ValueDataPoint: [tid: 6 | time: 100 | val: 6.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 200 | val: 1.0 | si: 100], ValueDataPoint: [tid: 6 | time: 200 | val: 6.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 300 | val: 1.0 | si: 100], ValueDataPoint: [tid: 6 | time: 300 | val: 6.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 400 | val: 1.0 | si: 100], ValueDataPoint: [tid: 6 | time: 400 | val: 6.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 500 | val: 1.0 | si: 100], ValueDataPoint: [tid: 6 | time: 500 | val: 6.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 600 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=200, valueDataPoints=[ValueDataPoint: [tid: 6 | time: 600 | val: 6.0 | si: 200]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 700 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 800 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=200, valueDataPoints=[ValueDataPoint: [tid: 6 | time: 800 | val: 6.0 | si: 200]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 900 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 1000 | val: 1.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=200, valueDataPoints=[ValueDataPoint: [tid: 6 | time: 1000 | val: 6.0 | si: 200]]}\n";
        assertOutputString(group, expected);
        //printAllSlices(group);
    }

    /*
    @Test
    void multipleConfigsInARow() {
        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(1);
        tsArray[1] = createTimeSeriesN(7);

        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        printAllSlices(group);
    }*/

    @Test
    void manyTimeSeries() {
        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[3];
        tsArray[0] = createTimeSeriesN(1);
        tsArray[1] = createTimeSeriesN(2);
        tsArray[2] = createTimeSeriesN(3);

        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        String expected =
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 100 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 100 | val: 2.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 200 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 200 | val: 2.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 300 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 300 | val: 2.0 | si: 100], ValueDataPoint: [tid: 3 | time: 300 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 400 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 400 | val: 2.0 | si: 100], ValueDataPoint: [tid: 3 | time: 400 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 500 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 500 | val: 2.0 | si: 100], ValueDataPoint: [tid: 3 | time: 500 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 600 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 600 | val: NaN | si: 100], ValueDataPoint: [tid: 3 | time: 600 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 700 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 700 | val: NaN | si: 100], ValueDataPoint: [tid: 3 | time: 700 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 800 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 800 | val: NaN | si: 100], ValueDataPoint: [tid: 3 | time: 800 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 900 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 900 | val: 2.0 | si: 100], ValueDataPoint: [tid: 3 | time: 900 | val: 3.0 | si: 100]]}\n" +
                "DataSlice{samplingInterval=100, valueDataPoints=[ValueDataPoint: [tid: 1 | time: 1000 | val: 1.0 | si: 100], ValueDataPoint: [tid: 2 | time: 1000 | val: 2.0 | si: 100], ValueDataPoint: [tid: 3 | time: 1000 | val: 3.0 | si: 100]]}\n";
        //printAllSlices(group);
        assertOutputString(group, expected);
    }


    @Test
    void getConfigurationDataPoints() {
    }
}