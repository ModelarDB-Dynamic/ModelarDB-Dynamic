package dk.aau.modelardb.core.GroupBasedCompression;

import MockData.CSVTimeSeriesProviderHelper;
import MockData.ConfigurationProvider;
import dk.aau.modelardb.core.model.DataSlice;
import dk.aau.modelardb.core.timeseries.TimeSeriesCSV;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TimeSeriesGroupTest {

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

    private void printAllSlices(TimeSeriesGroup group) {
        while (group.hasNext()) {
            DataSlice slice = group.getSlice();
            System.out.println(slice);
        }
    }

    @Test
    void getSlice() {
        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(1);
        tsArray[1] = createTimeSeriesN(2);

        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        printAllSlices(group);
    }

    @Test
    void getSliceOneStartsLater() {
        TimeSeriesGroup group;
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = createTimeSeriesN(1);
        tsArray[1] = createTimeSeriesN(3);

        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();

        printAllSlices(group);
    }

    @Test
    void getConfigurationDataPoints() {
    }
}