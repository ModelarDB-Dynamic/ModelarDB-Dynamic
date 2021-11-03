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
package dk.aau.modelardb.core.Models;

public class ValueDataPoint extends DataPoint implements Comparable<ValueDataPoint>{
    @Override
    public int compareTo(ValueDataPoint o) {
        return Integer.compare(this.getTid(), o.getTid());
    }

    /**
     * Instance Variables
     **/
    public final long timestamp;
    public final float value;
    public final int samplingInterval;
    /**
     * Constructors
     **/
    public ValueDataPoint(int tid, long timestamp, float value, int samplingInterval) {
        super(tid);
        this.timestamp = timestamp;
        this.value = value;
        this.samplingInterval = samplingInterval;
    }

    /**
     * Public Methods
     **/
    public String toString() {
        return "DataPoint: [" + this.getTid() + " | " + new java.sql.Timestamp(this.timestamp) + " | " + this.value + " | " + this.samplingInterval + "]";
    }

    public boolean isGapPoint(){
        return Float.isNaN(this.value);
    }

    @Override
    public boolean isConfigurationDataPoint() {
        return false;
    }

}
