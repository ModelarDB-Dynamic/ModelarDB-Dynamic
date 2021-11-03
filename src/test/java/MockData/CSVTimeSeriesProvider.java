package MockData;

import dk.aau.modelardb.core.timeseries.TimeSeriesCSV;

public class CSVTimeSeriesProvider {

    // Private constructor to ensure people cannot create instances of this class
    private CSVTimeSeriesProvider() { }

    private static TimeSeriesCSV createTimeSeries(String fileName, int tid, int si) {
        String csvSeparator = ",";
        boolean hasHeader = false;
        int timeStampColumnIndex = 0;
        String dateFormat = "java";
        String timeZone = "UTC";
        int valueColumnIndex = 1;
        String locale = "en";
        String relativePath = "src/test/java/MockData/";
        String path = relativePath + fileName;

        return new TimeSeriesCSV(path, tid, si, csvSeparator, hasHeader, timeStampColumnIndex, dateFormat,
                                 timeZone, valueColumnIndex, locale);
    }

    public static TimeSeriesCSV createTimeSeries1() {
        String fileName = "time_series_1_data.csv";
        return  createTimeSeries(fileName, 1, 100);
    }


    public static TimeSeriesCSV createEmptyTimeSeries() {
        String path = "empty_data.csv";
        return  createTimeSeries(path, 10, 100);
    }

}
