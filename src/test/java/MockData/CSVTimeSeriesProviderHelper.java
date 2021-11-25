package MockData;

import dk.aau.modelardb.core.timeseries.TimeSeriesCSV;

public class CSVTimeSeriesProviderHelper {

    // Private constructor to ensure people cannot create instances of this class
    private CSVTimeSeriesProviderHelper() {}

    public static TimeSeriesCSV createTimeSeries(String path, int tid) {
        String csvSeparator = ",";
        boolean hasHeader = false;
        int timeStampColumnIndex = 0;
        String dateFormat = "java";
        String timeZone = "UTC";
        int valueColumnIndex = 1;
        String locale = "en";

        return new TimeSeriesCSV(path, tid, csvSeparator, hasHeader, timeStampColumnIndex, dateFormat,
                                 timeZone, valueColumnIndex, locale);
    }
}
