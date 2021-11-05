package MockData;

import dk.aau.modelardb.core.timeseries.TimeSeriesCSV;

public class CSVTimeSeriesProvider {

    // Private constructor to ensure people cannot create instances of this class
    private CSVTimeSeriesProvider() { }

    private static TimeSeriesCSV createTimeSeries(String fileName, int tid) {
        String csvSeparator = ",";
        boolean hasHeader = false;
        int timeStampColumnIndex = 0;
        String dateFormat = "java";
        String timeZone = "UTC";
        int valueColumnIndex = 1;
        String locale = "en";
        String relativePath = "src/test/java/MockData/";
        String path = relativePath + fileName;

        return new TimeSeriesCSV(path, tid, csvSeparator, hasHeader, timeStampColumnIndex, dateFormat,
                                 timeZone, valueColumnIndex, locale);
    }

    public static TimeSeriesCSV createSimpleTimeSeries() {
        String fileName = "simple_time_series.csv";
        return  createTimeSeries(fileName, 1);
    }

    public static TimeSeriesCSV createTimeSeriesWithGaps() {
        String fileName = "time_series_with_gaps.csv";
        return  createTimeSeries(fileName, 2);
    }

    public static TimeSeriesCSV createTimeSeriesStartsWithConfigPoints() {
        String fileName = "time_series_starts_with_config.csv";
        return  createTimeSeries(fileName, 3);
    }

    public static TimeSeriesCSV createTimeSeriesWithConfigPoints() {
        String fileName = "time_series_with_config.csv";
        return  createTimeSeries(fileName, 4);
    }

    public static TimeSeriesCSV createEmptyTimeSeries() {
        String path = "empty_time_series.csv";
        return  createTimeSeries(path, 10);
    }

    public static TimeSeriesCSV createTimeSeries1() {
        String path = "time_series_1.csv";
        return  createTimeSeries(path, 101);
    }

    public static TimeSeriesCSV createTimeSeries2() {
        String path = "time_series_2.csv";
        return  createTimeSeries(path, 102);
    }

}
