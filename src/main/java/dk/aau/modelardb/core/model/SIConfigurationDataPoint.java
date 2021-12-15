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
package dk.aau.modelardb.core.model;

import java.util.Objects;

public class SIConfigurationDataPoint extends DataPoint {

    private final String configurationKey;
    private final int newSamplingInterval;
    private final int previousSamplingInterval;

    public SIConfigurationDataPoint(int timeSeriesId, int newSamplingInterval, int previousSamplingInterval) {
        super(timeSeriesId);
        this.configurationKey = "SI";
        this.newSamplingInterval = newSamplingInterval;
        this.previousSamplingInterval = previousSamplingInterval;
    }


    public static boolean isAConfigurationDataPoint(String timeSeriesKey) {
        return "SI".equals(timeSeriesKey);
    }

    public String getConfigurationKey() {
        return configurationKey;
    }

    public int getNewSamplingInterval() {
        return newSamplingInterval;
    }

    public int getPreviousSamplingInterval() {
        return previousSamplingInterval;
    }

    public boolean hasPreviousSamplingInterval() {
        return previousSamplingInterval != Integer.MIN_VALUE;
    }

    public String toString() {
        String previousSIstring;
        if (!hasPreviousSamplingInterval()) {
            previousSIstring = "No previous SI";
        } else {
            previousSIstring = Integer.toString(previousSamplingInterval);
        }

        return "SIConfigurationDataPoint: [" + this.getTid() + " | " + newSamplingInterval + " | " + previousSIstring + "]";
    }

    @Override
    public boolean isConfigurationDataPoint() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) {
            return false;
        }
        SIConfigurationDataPoint that = (SIConfigurationDataPoint) o;
        return newSamplingInterval == that.newSamplingInterval && previousSamplingInterval == that.previousSamplingInterval && configurationKey.equals(that.configurationKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configurationKey, newSamplingInterval, previousSamplingInterval);
    }
}
