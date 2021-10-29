/* Copyright 2018 The ModelarDB Contributors
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
package dk.aau.modelardb.core.timeseries;

import dk.aau.modelardb.core.DataPoint;
import dk.aau.modelardb.core.SIConfigurationDataPoint;
import dk.aau.modelardb.core.ValueDataPoint;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

public class TimeSeriesCSV extends TimeSeries {

    private final boolean hasHeader;
    private final float scalingFactor;
    private final int bufferSize;
    private final StringBuffer decodeBuffer;
    private final String csvSeparator;
    private final int timestampColumnIndex;
    private final int dateParserType;
    private final int valueColumnIndex;
    private final NumberFormat valueParser;
    /**
     * Instance Variables
     **/
    private String stringPath;
    private ByteBuffer byteBuffer;
    private StringBuffer nextBuffer;
    private ReadableByteChannel channel;
    private SimpleDateFormat dateParser;
    private long nextTimestampPointer;

    /**
     * Public Methods
     **/
    public TimeSeriesCSV(String stringPath, int tid, int initialSamplingInterval,
                         String csvSeparator, boolean hasHeader,
                         int timestampColumnIndex, String dateFormat, String timeZone,
                         int valueColumnIndex, String localeString) {
        super(stringPath.substring(stringPath.lastIndexOf('/') + 1), tid, initialSamplingInterval);
        this.stringPath = stringPath;

        //A small buffer is used so more time series can be ingested in parallel
        this.bufferSize = 1024;
        this.hasHeader = hasHeader;
        this.csvSeparator = csvSeparator;
        this.scalingFactor = 1.0F;


        this.timestampColumnIndex = timestampColumnIndex;
        switch (dateFormat) {
            case "unix":
                this.dateParserType = 1;
                break;
            case "java":
                this.dateParserType = 2;
                break;
            default:
                this.dateParserType = 3;
                this.dateParser = new SimpleDateFormat(dateFormat);
                this.dateParser.setTimeZone(java.util.TimeZone.getTimeZone(timeZone));
                this.dateParser.setLenient(false);
                break;
        }

        this.valueColumnIndex = valueColumnIndex;
        Locale locale = new Locale(localeString);
        this.valueParser = NumberFormat.getInstance(locale);
        this.decodeBuffer = new StringBuffer();
        this.nextBuffer = new StringBuffer();
    }

    public void open() throws RuntimeException {
        try {
            FileChannel fc = FileChannel.open(Paths.get(this.stringPath));

            //Wraps the channel in a stream if the data is compressed
            String suffix = "";
            int lastIndexOfDot = this.stringPath.lastIndexOf('.');
            if (lastIndexOfDot > -1) {
                suffix = this.stringPath.substring(lastIndexOfDot);
            }
            stringPath = null;

            if (".gz".equals(suffix)) {
                InputStream is = Channels.newInputStream(fc);
                GZIPInputStream gis = new GZIPInputStream(is);
                this.channel = Channels.newChannel(gis);
            } else {
                this.channel = fc;
            }
            this.byteBuffer = ByteBuffer.allocate(this.bufferSize);
            if (this.hasHeader) {
                readLines();
                this.nextBuffer.delete(0, this.nextBuffer.indexOf("\n") + 1);
            }
        } catch (IOException ioe) {
            //An unchecked exception is used so the function can be called in a lambda function
            throw new RuntimeException(ioe);
        }
    }

    public ValueDataPoint next() {
        try {
            if (this.nextBuffer.length() == 0) {
                readLines();
            }
            return nextDataPoint();
        } catch (IOException ioe) {
            close();
            throw new java.lang.RuntimeException(ioe);
        }
    }

    public boolean hasNext() {
        try {
            if (this.nextBuffer.length() == 0) {
                readLines();
            }
            return this.nextBuffer.length() != 0;
        } catch (IOException ioe) {
            close();
            throw new java.lang.RuntimeException(ioe);
        }
    }

    public String toString() {
        return "Time Series: [" + this.tid + " | " + this.source + " | " + this.currentSamplingInterval + "]";
    }

    public void close() {
        //If the channel was never initialized there is nothing to close
        if (this.channel == null) {
            return;
        }

        try {
            this.channel.close();
            //Clears all references to channels and buffers to enable garbage collection
            this.byteBuffer = null;
            this.nextBuffer = null;
            this.channel = null;
        } catch (IOException ioe) {
            throw new java.lang.RuntimeException(ioe);
        }
    }

    /**
     * Private Methods
     **/
    private void readLines() throws IOException {
        //Reads until the channel no longer provides any bytes or at least one full data point have been read
        int bytesRead;
        do {
            this.byteBuffer.clear();
            bytesRead = this.channel.read(this.byteBuffer);
            this.byteBuffer.flip();
            this.decodeBuffer.append(StandardCharsets.UTF_8.decode(this.byteBuffer));
        } while (bytesRead != -1 && this.decodeBuffer.indexOf("\n") == -1);

        //Transfer all fully read data points into a new buffer to simplify the remaining implementation
        int lastFullyParsedDataPoint = this.decodeBuffer.lastIndexOf("\n") + 1;
        this.nextBuffer.append(this.decodeBuffer, 0, lastFullyParsedDataPoint);
        this.decodeBuffer.delete(0, lastFullyParsedDataPoint);
    }


    private ValueDataPoint nextDataPoint() throws IOException {
        try {
            int nextDataPointIndex = this.nextBuffer.indexOf("\n") + 1;
            String[] split;
            if (nextDataPointIndex == 0) {
                split = this.nextBuffer.toString().split(csvSeparator);
            } else {
                split = this.nextBuffer.substring(0, nextDataPointIndex).split(csvSeparator);
            }


            ValueDataPoint result;
            if (SIConfigurationDataPoint.isAConfigurationDataPoint(split[0])) {
                int configurationValue = Integer.parseInt(split[1]);
                String configurationKey = split[0];
                if ("SI".equals(configurationKey)) {
                    this.currentSamplingInterval = configurationValue;
                    this.nextTimestampPointer += (this.nextTimestampPointer%this.currentSamplingInterval);
                }
                if (nextDataPointIndex != 0) {//delete the config datapoint that have been read from the buffer
                    this.nextBuffer.delete(0, nextDataPointIndex);
                }
                result = nextDataPoint();
            } else {
                //Parses the timestamp column as either Unix time, Java time, or a human readable timestamp
                long timestamp = 0;
                switch (this.dateParserType) {
                    case 1:
                        //Unix time
                        timestamp = new Date(Long.parseLong(split[timestampColumnIndex]) * 1000).getTime();
                        break;
                    case 2:
                        //Java time
                        timestamp = new Date(Long.parseLong(split[timestampColumnIndex])).getTime();
                        break;
                    case 3:
                        //Human readable timestamp
                        timestamp = dateParser.parse(split[timestampColumnIndex]).getTime();
                        break;
                }
                float dataPointValue;
                if (nextTimestampPointer == timestamp) {
                    dataPointValue = valueParser.parse(split[valueColumnIndex]).floatValue();
                    if (nextDataPointIndex != 0) {//delete the data point from the buffer
                        this.nextBuffer.delete(0, nextDataPointIndex);
                    }
                } else {
                    //datapoint from buffer is not deleted as the timestamp did not match the next pointer. Therefore a gap datapoint is emitted at the expected timestamp
                    dataPointValue = Float.NaN; // value of Nan indicates Gap
                }
                result = new ValueDataPoint(this.tid, timestamp, this.scalingFactor * dataPointValue, this.currentSamplingInterval);
                this.nextTimestampPointer += this.currentSamplingInterval;
            }
            return result;
        } catch (ParseException pe) {
            //If the input cannot be parsed the stream is considered empty
            this.channel.close();
            throw new java.lang.RuntimeException(pe);
        }
    }
}
