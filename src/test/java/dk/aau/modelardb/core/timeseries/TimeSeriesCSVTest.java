package dk.aau.modelardb.core.timeseries;

import MockData.CSVTimeSeriesProvider;
import MockData.ConfigurationProvider;
import dk.aau.modelardb.core.Configuration;
import dk.aau.modelardb.core.Models.DataPoint;
import dk.aau.modelardb.core.Models.SIConfigurationDataPoint;
import dk.aau.modelardb.core.Models.ValueDataPoint;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

class TimeSeriesCSVTest {
    TimeSeriesCSV ts;

    @BeforeAll
    static void init() {
        ConfigurationProvider.setDefaultValuesForConfigurationInstance();
    }

    @BeforeEach
    void setup() {
        ts = CSVTimeSeriesProvider.createTimeSeries1();
        ts.open();
    }

    @AfterEach
    void cleanup() {
        ts.close();
    }

    @Test
    void hasNextOnOpened() {
        Assertions.assertTrue(ts.hasNext());
    }

    @Test
    void hasNextOnUnopened() {
        ts.close();
        Assertions.assertThrows(RuntimeException.class, () -> ts.hasNext());
    }

    @Test
    void hasNextOnEmptyTs() {
        TimeSeriesCSV emptyTimeSeries = CSVTimeSeriesProvider.createEmptyTimeSeries();
        emptyTimeSeries.open();

        Assertions.assertFalse(emptyTimeSeries.hasNext());
    }

    @Test
    void nextFirstReturnsDefaultConfigPoint() {

        var point = ts.next();
        Assertions.assertTrue(point instanceof SIConfigurationDataPoint);
        SIConfigurationDataPoint configPoint = (SIConfigurationDataPoint) point;

        Assertions.assertEquals("SI", configPoint.getConfigurationKey());
        Assertions.assertEquals(1, point.getTid());
        Assertions.assertEquals(Configuration.INSTANCE.getSamplingInterval(), configPoint.getNewSamplingInterval());
        Assertions.assertFalse(configPoint.hasPreviousSamplingInterval());
    }

    @Test
    void nextReturnsCorrectValuePoints() {
        List<ValueDataPoint> expectedDataPoints = new ArrayList<>();
        int tid = ts.tid;
        int si = ts.getCurrentSamplingInterval();
        expectedDataPoints.add(new ValueDataPoint(tid, 100, 1.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 200, 1.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 300, 1.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 400, 1.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 500, 1.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 600, 5.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 700, 5.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 800, 5.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 900, 5.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 1000, 5.0F, si));

        // Reads out the config point and then ignores it
        var point = ts.next();
        Assertions.assertTrue(point instanceof SIConfigurationDataPoint);

        int i = 0;
        while (ts.hasNext()) {
            point = ts.next();
            Assertions.assertTrue(point instanceof ValueDataPoint);
            var valuePoint = (ValueDataPoint)point;
            Assertions.assertEquals(expectedDataPoints.get(i), valuePoint);
            i++;
        }
    }

    @Test
    void timeSeriesWithConfigPoint() {
        // TODO CREATE THIS BY MAKING A NEW TIME SERIES WITH CONFIG POINT IN IT

    }

}