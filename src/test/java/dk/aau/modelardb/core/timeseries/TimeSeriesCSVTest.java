package dk.aau.modelardb.core.timeseries;

import MockData.CSVTimeSeriesProviderHelper;
import MockData.ConfigurationProvider;
import dk.aau.modelardb.core.Configuration;
import dk.aau.modelardb.core.model.DataPoint;
import dk.aau.modelardb.core.model.SIConfigurationDataPoint;
import dk.aau.modelardb.core.model.ValueDataPoint;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

class TimeSeriesCSVTest {
    TimeSeriesCSV ts;
    private static final String relativePath = "src/test/java/dk/aau/modelardb/core/timeseries/";

    public static TimeSeriesCSV createSimpleTimeSeries() {
        String fileName = "simple_time_series.csv";
        return  CSVTimeSeriesProviderHelper.createTimeSeries(relativePath + fileName);
    }

    public static TimeSeriesCSV createTimeSeriesWithGaps() {
        String fileName = "time_series_with_gaps.csv";
        return  CSVTimeSeriesProviderHelper.createTimeSeries(relativePath + fileName);
    }

    public static TimeSeriesCSV createTimeSeriesStartsWithConfigPoints() {
        String fileName = "time_series_starts_with_config.csv";
        return CSVTimeSeriesProviderHelper.createTimeSeries(relativePath + fileName);
    }

    public static TimeSeriesCSV createTimeSeriesWithConfigPoints() {
        String fileName = "time_series_with_config.csv";
        return CSVTimeSeriesProviderHelper.createTimeSeries(relativePath + fileName);
    }

    public static TimeSeriesCSV createEmptyTimeSeries() {
        String path = "empty_time_series.csv";
        return CSVTimeSeriesProviderHelper.createTimeSeries(relativePath + path);
    }

    @BeforeAll
    static void init() {
        ConfigurationProvider.setDefaultValuesForConfigurationInstance();
    }

    @AfterAll
    static void cleanup(){
        ConfigurationProvider.removeDefaultValues();
    }

    @BeforeEach
    void setup() {
        ts = createSimpleTimeSeries();
        ts.open();
    }

    @AfterEach
    void closeTS() {
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
        TimeSeriesCSV emptyTimeSeries = createEmptyTimeSeries();
        emptyTimeSeries.open();

        Assertions.assertFalse(emptyTimeSeries.hasNext());
    }

    @Test
    void nextFirstReturnsDefaultConfigPoint() {

        var point = ts.next();
        Assertions.assertTrue(point instanceof SIConfigurationDataPoint);
        SIConfigurationDataPoint configPoint = (SIConfigurationDataPoint) point;

        Assertions.assertEquals("SI", configPoint.getConfigurationKey());
        Assertions.assertEquals(ts.tid, point.getTid());
        Assertions.assertEquals(Configuration.INSTANCE.getSamplingInterval(), configPoint.getNewSamplingInterval());
        Assertions.assertFalse(configPoint.hasPreviousSamplingInterval());
    }

    private SIConfigurationDataPoint createDefaultConfigPoint(int tid) {
        return new SIConfigurationDataPoint(tid, Configuration.INSTANCE.getSamplingInterval(), Integer.MIN_VALUE);
    }

    private void printOutTimeSeries(TimeSeriesCSV timeSeries) {
        while (timeSeries.hasNext()) {
            var point = timeSeries.next();
            System.out.println(point);
        }
    }

    private void checkTimeSeriesAgainstExpectedList(List<DataPoint> expectedDataPoints, TimeSeriesCSV timeSeries)  {
        int i = 0;
        while (timeSeries.hasNext()) {
            var point = timeSeries.next();
            var expectedPoint = expectedDataPoints.get(i);
            if (point instanceof ValueDataPoint && expectedPoint instanceof ValueDataPoint) {
                // TODO: check if this is necesssary
                var valuePoint = (ValueDataPoint)point;
                var expectedValuePoint = (ValueDataPoint)expectedPoint;
                Assertions.assertEquals(expectedValuePoint, valuePoint);
            } else if (point instanceof SIConfigurationDataPoint && expectedPoint instanceof SIConfigurationDataPoint) {
                var configPoint = (SIConfigurationDataPoint)point;
                var expectedConfigPoint = (SIConfigurationDataPoint)expectedPoint;
                Assertions.assertEquals(expectedConfigPoint, configPoint);
            } else {
                throw new IllegalArgumentException("The types of the actual and expected data points did not match for: i = " +  i);
            }
            i++;
        }
    }

    @Test
    void simpleTimeSeries() {
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        int tid = ts.tid;
        int si = Configuration.INSTANCE.getSamplingInterval();;
        expectedDataPoints.add(createDefaultConfigPoint(tid));
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

        checkTimeSeriesAgainstExpectedList(expectedDataPoints, ts);
    }

    @Test
    void timeSeriesWithGaps() {
        ts = createTimeSeriesWithGaps();
        ts.open();
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        int tid = ts.tid;
        int si = Configuration.INSTANCE.getSamplingInterval();
        expectedDataPoints.add(createDefaultConfigPoint(tid));
        expectedDataPoints.add(new ValueDataPoint(tid, 100, 1.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 200, 1.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 300, Float.NaN, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 400, Float.NaN, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 500, 1.0F, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 600, Float.NaN, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 700, Float.NaN, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 800, Float.NaN, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 900, Float.NaN, si));
        expectedDataPoints.add(new ValueDataPoint(tid, 1000, 1.0F, si));

        checkTimeSeriesAgainstExpectedList(expectedDataPoints, ts);
    }

    @Test
    void timeSeriesStartsWithConfig() {
        ts = createTimeSeriesStartsWithConfigPoints();
        ts.open();

        List<DataPoint> expectedDataPoints = new ArrayList<>();
        int tid = ts.tid;
        int newSi = 50;
        expectedDataPoints.add(createDefaultConfigPoint(tid));
        expectedDataPoints.add(new SIConfigurationDataPoint(tid, newSi, Configuration.INSTANCE.getSamplingInterval()));
        expectedDataPoints.add(new ValueDataPoint(tid, 0, 1.0F, newSi));
        expectedDataPoints.add(new ValueDataPoint(tid, 50, 1.0F, newSi));
        expectedDataPoints.add(new ValueDataPoint(tid, 100, 1.0F, newSi));
        expectedDataPoints.add(new ValueDataPoint(tid, 150, 1.0F, newSi));
        expectedDataPoints.add(new ValueDataPoint(tid, 200, 1.0F, newSi));
        expectedDataPoints.add(new ValueDataPoint(tid, 250, 1.0F, newSi));

        checkTimeSeriesAgainstExpectedList(expectedDataPoints, ts);
    }

    @Test
    void timeSeriesWithConfigPoint() {
        ts = createTimeSeriesWithConfigPoints();
        ts.open();

        List<DataPoint> expectedDataPoints = new ArrayList<>();
        int tid = ts.tid;
        int currSi = Configuration.INSTANCE.getSamplingInterval();
        expectedDataPoints.add(createDefaultConfigPoint(tid));
        expectedDataPoints.add(new ValueDataPoint(tid, 100, 1.0F, currSi));
        expectedDataPoints.add(new ValueDataPoint(tid, 200, 1.0F, currSi));
        expectedDataPoints.add(new ValueDataPoint(tid, 300, 1.0F, currSi));
        int newSi = 200;
        expectedDataPoints.add(new SIConfigurationDataPoint(tid, newSi, currSi));
        currSi = newSi;
        expectedDataPoints.add(new ValueDataPoint(tid, 400, 2.0F, currSi));
        expectedDataPoints.add(new ValueDataPoint(tid, 600, 2.0F, currSi));
        expectedDataPoints.add(new ValueDataPoint(tid, 800, 2.0F, currSi));
        newSi = 100;
        expectedDataPoints.add(new SIConfigurationDataPoint(tid, newSi, currSi));
        currSi = newSi;
        expectedDataPoints.add(new ValueDataPoint(tid, 900, 3.0F, currSi));
        expectedDataPoints.add(new ValueDataPoint(tid, 1000, 3.0F, currSi));

        checkTimeSeriesAgainstExpectedList(expectedDataPoints, ts);
    }

}