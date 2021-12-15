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
package dk.aau.modelardb.core.GroupBasedCompression;

import dk.aau.modelardb.core.model.compression.ModelType;
import dk.aau.modelardb.core.utility.Logger;
import dk.aau.modelardb.core.utility.SegmentFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class SegmentGeneratorSupplier {
    private final TimeSeriesGroup timeSeriesGroup;
    private final Supplier<ModelType[]> modelTypeInitializer;
    private final ModelType fallbackModelType;
    private final int maximumLatency;
    private final SegmentFunction temporarySegmentStream;
    private final SegmentFunction finalizedSegmentStream;
    private final Set<Integer> allTids;
    private final float dynamicSplitFraction;
    private final Logger segmentGeneratorLogger;

    public SegmentGeneratorSupplier(TimeSeriesGroup timeSeriesGroup, Supplier<ModelType[]> modelTypeInitializer,
                                    ModelType fallbackModelType, int maximumLatency, SegmentFunction temporarySegmentStream,
                                    SegmentFunction finalizedSegmentStream, float dynamicSplitFraction, Logger logger) {
        this.timeSeriesGroup = timeSeriesGroup;
        this.modelTypeInitializer = modelTypeInitializer;
        this.fallbackModelType = fallbackModelType;
        this.allTids = timeSeriesGroup.getTids();
        this.maximumLatency = maximumLatency;
        this.dynamicSplitFraction = dynamicSplitFraction;
        this.temporarySegmentStream = temporarySegmentStream;
        this.finalizedSegmentStream = finalizedSegmentStream;
        this.segmentGeneratorLogger = logger;
    }

    public SegmentGenerator get(List<Integer> tids, int si) {
        Set<Integer> tidSet = new HashSet<>(tids);
        Set<Integer> permanentGapTids = new HashSet<>(this.allTids);
        permanentGapTids.removeAll(tidSet);

        return new SegmentGenerator(this.timeSeriesGroup.gid, si, permanentGapTids, modelTypeInitializer, fallbackModelType, tids, maximumLatency, dynamicSplitFraction, temporarySegmentStream, finalizedSegmentStream, segmentGeneratorLogger);
    }
}
