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

import dk.aau.modelardb.core.GroupBasedCompression.SegmentGroup;
import dk.aau.modelardb.core.utility.SegmentFunction;

import java.util.ArrayList;
import java.util.List;

public class MockSegmentFunction implements SegmentFunction {
    private final List<SegmentGroup> segments;

    public MockSegmentFunction() {
        this.segments = new ArrayList<>();
    }

    @Override
    public void emit(int gid, long startTime, int samplingInterval, long endTime, int mtid, byte[] model, byte[] gaps) {
        SegmentGroup segmentGroup = new SegmentGroup(gid, startTime, samplingInterval, endTime, mtid, model, gaps);
        segments.add(segmentGroup);
    }

    public List<SegmentGroup> getSegments() {
        return segments;
    }
}

