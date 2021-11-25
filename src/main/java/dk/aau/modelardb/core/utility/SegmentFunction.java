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
package dk.aau.modelardb.core.utility;

import dk.aau.modelardb.core.model.ValueDataPoint;
import dk.aau.modelardb.core.model.compression.ModelType;

import java.util.ArrayList;
import java.util.List;

public interface SegmentFunction {

    default void emit(ArrayList<ValueDataPoint[]> buffer, ModelType modelType, List<Integer> segmentGaps, int samplingInterval, int gid) {
        if(buffer.size() == 0) {
            return;
        }
        int amtPointsInModel = modelType.length();
        long startTime = buffer.get(0)[0].timestamp;
        long endTime = buffer.get(amtPointsInModel - 1)[0].timestamp;
        int[] gaps = segmentGaps.stream().mapToInt(l -> l).toArray();
        byte[] model = modelType.getModel(startTime, endTime, samplingInterval, buffer);
        emit(gid, startTime, samplingInterval, endTime, modelType.mtid, model, Static.intToBytes(gaps));
    }

    void emit(int gid, long startTime, int samplingInterval, long endTime, int mtid, byte[] model, byte[] gaps);
}
