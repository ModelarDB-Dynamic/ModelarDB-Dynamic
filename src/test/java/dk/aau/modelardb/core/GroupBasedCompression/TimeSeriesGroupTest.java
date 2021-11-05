package dk.aau.modelardb.core.GroupBasedCompression;

import MockData.CSVTimeSeriesProvider;
import MockData.ConfigurationProvider;
import dk.aau.modelardb.core.model.DataSlice;
import dk.aau.modelardb.core.timeseries.TimeSeriesCSV;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TimeSeriesGroupTest {
    TimeSeriesGroup group;

    @BeforeAll
    static void init() {
        ConfigurationProvider.setDefaultValuesForConfigurationInstance();
    }

    @BeforeEach
    void setup() {
        TimeSeriesCSV[] tsArray = new TimeSeriesCSV[2];
        tsArray[0] = CSVTimeSeriesProvider.createTimeSeries1();
        tsArray[1] = CSVTimeSeriesProvider.createTimeSeries2();

        group = new TimeSeriesGroup(1, tsArray);
        group.initialize();
    }




    @Test
    void getTimeSeries() {
    }

    @Test
    void getTids() {
    }

    @Test
    void getSources() {
    }

    @Test
    void size() {
    }

    @Test
    void hasNext() {
    }

    @Test
    void getSlice() {

        while(group.hasNext()) {
            DataSlice slice = group.getSlice();
            System.out.println(slice);
        }

    }

    @Test
    void getActiveTimeSeries() {
    }

    @Test
    void getConfigurationDataPoints() {
    }
}