/* Copyright 2021 The ModelarDB-Dynamic Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
