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
package dk.aau.modelardb.core.model.compression;

import dk.aau.modelardb.core.model.ValueDataPoint;
import dk.aau.modelardb.core.utility.Static;

import java.nio.ByteBuffer;
import java.util.List;

class PMC_MeanModelType extends ModelType {

    /**
     * Instance Variables
     **/
    private int currentSize;
    private float min;
    private float max;
    private double sum;
    private boolean withinErrorBound;

    /**
     * Constructors
     **/
    PMC_MeanModelType(int mtid, float errorBound, int lengthBound) {
        super(mtid, errorBound, lengthBound);
        if (errorBound < 0.0 || 100.0 < errorBound) {
            throw new IllegalArgumentException("CORE: for PMC_MeanModelType modelardb.error_bound must be a percentage");
        }
    }

    /**
     * Public Methods
     **/
    @Override
    public boolean append(ValueDataPoint[] currentValueDataPoints) {
        if (!this.withinErrorBound) {
            return false;
        }

        //The model can represent the data points if the new average is the within error bound of the new min and max
        float nextMin = this.min;
        float nextMax = this.max;
        double nextSum = this.sum;
        for (ValueDataPoint cdp : currentValueDataPoints) {
            float value = cdp.value;
            nextSum += value;
            nextMin = Math.min(nextMin, value);
            nextMax = Math.max(nextMax, value);
        }

        float average = (float) (nextSum / ((this.currentSize + 1) * currentValueDataPoints.length));
        if (Static.outsidePercentageErrorBound(this.errorBound, average, nextMin) ||
                Static.outsidePercentageErrorBound(this.errorBound, average, nextMax)) {
            this.withinErrorBound = false;
            return false;
        }
        this.min = nextMin;
        this.max = nextMax;
        this.sum = nextSum;
        this.currentSize += 1;
        return true;
    }

    @Override
    public void initialize(List<ValueDataPoint[]> currentSegment) {
        this.sum = 0.0;
        this.currentSize = 0;
        this.min = Float.MAX_VALUE;
        this.max = -Float.MAX_VALUE;
        this.withinErrorBound = true;

        for (ValueDataPoint[] valueDataPoints : currentSegment) {
            if (!append(valueDataPoints)) {
                return;
            }
        }
    }

    @Override
    public byte[] getModel(long startTime, long endTime, int samplingInterval, List<ValueDataPoint[]> dps) {
        return ByteBuffer.allocate(4).putFloat((float) (this.sum / (this.currentSize * dps.get(0).length))).array();
    }

    @Override
    public Segment get(int gid, long startTime, int samplingInterval, long endTime, byte[] model, byte[] offsets) {
        return new PMC_MeanSegment(gid, startTime, samplingInterval, endTime, model, offsets);
    }

    @Override
    public int length() {
        return this.currentSize;
    }

    @Override
    public float size(long startTime, long endTime, int samplingInterval, List<ValueDataPoint[]> dps) {
        if (this.currentSize == 0) {
            return Float.NaN;
        } else {
            //The values are represented as a single float which is four bytes
            return 4.0F;
        }
    }
}


class PMC_MeanSegment extends Segment {

    /**
     * Instance Variables
     **/
    private final float value;

    /**
     * Constructors
     **/
    PMC_MeanSegment(int tid, long startTime, int samplingInterval, long endTime, byte[] model, byte[] offsets) {
        super(tid, startTime, samplingInterval, endTime, offsets);
        this.value = ByteBuffer.wrap(model).getFloat();
    }

    /**
     * Public Methods
     **/
    @Override
    public float min() {
        return this.value;
    }

    @Override
    public float max() {
        return this.value;
    }

    @Override
    public double sum() {
        return this.length() * this.value;
    }

    /**
     * Protected Methods
     **/
    protected float get(long timestamp, int index) {
        return this.value;
    }
}
