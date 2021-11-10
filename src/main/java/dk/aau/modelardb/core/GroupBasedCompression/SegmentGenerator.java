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
package dk.aau.modelardb.core.GroupBasedCompression;

import dk.aau.modelardb.core.model.compression.ModelType;
import dk.aau.modelardb.core.model.DataSlice;
import dk.aau.modelardb.core.model.ValueDataPoint;
import dk.aau.modelardb.core.utility.Logger;
import dk.aau.modelardb.core.utility.ReverseBufferIterator;
import dk.aau.modelardb.core.utility.SegmentFunction;
import dk.aau.modelardb.core.utility.Static;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SegmentGenerator {

    /**
     * Instance Variables
     **/
    //Variables from the constructor
    private final int gid;
    private final int maximumLatency;
    private final int samplingInterval;
    private final ModelType[] modelTypes;
    private final ModelType fallbackModelType;
    private final Supplier<ModelType[]> modelTypeInitializer;
    private final SegmentFunction finalizedSegmentStream;
    private final SegmentFunction temporarySegmentStream;
    private final Set<Integer> permanentGapTids;
    //State variables for controlling split generators
    private final List<Integer> tids;
    //DEBUG: logger instance, for counting segments, used for this generator
    Logger logger;
    //State variables for buffering data points
    private Set<Integer> gaps;
    private ArrayList<ValueDataPoint[]> buffer; // TODO: MAKE THIS CONTAIN A LIST OF SLICES INSTEAD
    private float dynamicSplitFraction;
    private long emittedFinalizedSegments;
    private double compressionRatioAverage;
    private long finalizedSegmentsBeforeNextJoinCheck;
    private Set<SegmentGenerator> splitsToJoinIfCorrelated;
    private List<SegmentGenerator> splitSegmentGenerators;
    //State variables for fitting the current model
    private int modelTypeIndex;
    private int slicesNotYetEmitted;
    private ModelType currentModelType;
    private ModelType lastEmittedModelType;

    private boolean finalized = false;


    /**
     * Constructors
     **/
    SegmentGenerator(int gid, int samplingInterval, Set<Integer> permenentGapTids, Supplier<ModelType[]> modelTypeInitializer,
                     ModelType fallbackModelType, List<Integer> tids, int maximumLatency, float dynamicSplitFraction,
                     SegmentFunction temporarySegmentStream, SegmentFunction finalizedSegmentStream) {

        //Variables from the constructor
        this.gid = gid;
        this.modelTypes = modelTypeInitializer.get();
        this.fallbackModelType = fallbackModelType;
        this.maximumLatency = maximumLatency;
        Collections.sort(tids);
        this.tids = Collections.unmodifiableList(tids);
        this.samplingInterval = samplingInterval;
        this.permanentGapTids = permenentGapTids;

        this.modelTypeInitializer = modelTypeInitializer;
        this.finalizedSegmentStream = finalizedSegmentStream;
        this.temporarySegmentStream = temporarySegmentStream;

        //State variables for controlling split generators
        this.emittedFinalizedSegments = 0;
        this.compressionRatioAverage = 0.0;
        this.finalizedSegmentsBeforeNextJoinCheck = 1;
        this.dynamicSplitFraction = dynamicSplitFraction;
        this.splitSegmentGenerators = new ArrayList<>();
        this.splitsToJoinIfCorrelated = new HashSet<>();

        //State variables for buffering data points
        this.gaps = new HashSet<>();
        this.buffer = new ArrayList<>();

        //State variables for fitting the current model
        this.modelTypeIndex = 0;
        this.slicesNotYetEmitted = 0;
        this.currentModelType = this.modelTypes[0];
        this.currentModelType.initialize(this.buffer);

        //DEBUG: logger instance for counting segments used for this generator
        this.logger = new Logger(new Random().nextInt(1000));
    }

    public boolean isFinalized() {
        return finalized;
    }

    void close() {
        if (finalized) {
            return;
        }
        this.finalized = true;
        for (SegmentGenerator sg : this.splitSegmentGenerators) {
            sg.flushBuffer();
        }
        flushBuffer();
    }

    public void consumeSlice(DataSlice slice) {
        // This is necessary in cases where for example one of the time series ends early
        slice.addGapsForTidsWithMissingPoints(new HashSet<>(this.tids));

        if (this.splitSegmentGenerators.isEmpty() || this.splitSegmentGenerators.contains(this)) {
            // Consume
            consumeDataPoints(slice.getDataPoints());
        } else { // Delegate slices to children
            Map<SegmentGenerator, HashSet<Integer>> segmentGeneratorToTids = this.splitSegmentGenerators.stream()
                    .map(segmentGenerator -> Pair.of(segmentGenerator, new HashSet<>(segmentGenerator.tids)))
                    .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

            Map<Set<Integer>, DataSlice> dataSliceByTids = slice.getSubDataSlices(new HashSet<>(segmentGeneratorToTids.values()));

            for (Map.Entry<SegmentGenerator, HashSet<Integer>> segmentGeneratorTidsPair : segmentGeneratorToTids.entrySet()) {
                DataSlice dataSliceForSubGenerator = dataSliceByTids.get(segmentGeneratorTidsPair.getValue());
                segmentGeneratorTidsPair.getKey().consumeSlice(dataSliceForSubGenerator);
            }

            joinGroupsIfTheirTimeSeriesAreCorrelated();
        }
    }

    private void consumeDataPoints(ValueDataPoint[] dataPoints) {
        if (dataPoints.length == 0) {
            return;
        }

        // Handle gap data points
        for (ValueDataPoint dataPoint : dataPoints) {
            if (dataPoint.isGapPoint()) {
                if (!this.gaps.contains(dataPoint.getTid())) {
                    flushBuffer();
                    this.gaps.add(dataPoint.getTid());
                }
            } else {
                if (this.gaps.contains(dataPoint.getTid())) {
                    // a gap has ended
                    flushBuffer();
                    this.gaps.remove(dataPoint.getTid());
                }
            }
        }

        ValueDataPoint[] gapFreeDatapoints = Arrays.stream(dataPoints)
                .filter(Predicate.not(ValueDataPoint::isGapPoint))
                .toArray(ValueDataPoint[]::new);

        boolean couldAppendSlice = tryToAppendDataPointsToModels(gapFreeDatapoints);

        this.slicesNotYetEmitted++;
        this.buffer.add(gapFreeDatapoints);

        if (!couldAppendSlice) {
            emitFinalSegment();
            resetModelTypeIndex();
        }

        //Emits a temporary segment if latency data points have been added to the buffer without a finalized segment being
        // emitted, if the current model does not represent all of the data points in the buffer the fallback model is used
        if (this.maximumLatency > 0 && this.slicesNotYetEmitted == this.maximumLatency) {
            emitTemporarySegment();
            this.slicesNotYetEmitted = 0;
        }
    }

    private boolean tryToAppendDataPointsToModels(ValueDataPoint[] gapFreeDatapoints) {
        //The current model type is given the data points and it verifies that the model can represent them and all prior,
        // it is assumed that append will fail if it failed in the past, so append(t,V) must fail if append(t-1,V) failed
        while(!this.currentModelType.append(Arrays.copyOf(gapFreeDatapoints, gapFreeDatapoints.length))) {
            this.modelTypeIndex += 1;
            if (this.modelTypeIndex == this.modelTypes.length) {
                //If none of the model types can represent all of the buffered data points, the model type that provides
                // the best compression is selected and a segment that store the values a model of the type is construct
                return false;
            } else {
                this.currentModelType = this.modelTypes[this.modelTypeIndex];
                this.currentModelType.initialize(new ArrayList<>(this.buffer));
            }
        }
        return true;
    }


    //** Private Methods **/
    private void flushBuffer() {
        //If no data points are currently stored in the buffer it cannot be flushed
        if (this.buffer.isEmpty()) {
            return;
        }

        //Any uninitialized model types must be initialized before the buffer is flushed
        for (this.modelTypeIndex += 1; this.modelTypeIndex < this.modelTypes.length; this.modelTypeIndex++) {
            modelTypes[this.modelTypeIndex].initialize(this.buffer);
        }

        //Finalized segments are emitted until the buffer is empty, dynamic splitting is disabled as flushing can
        // create models with a poor compression ratio despite the time series in the group still being correlated
        float previousDynamicSplitFraction = this.dynamicSplitFraction;
        this.dynamicSplitFraction = 0;
        while (!buffer.isEmpty()) {
            emitFinalSegment();
            for (ModelType m : this.modelTypes) {
                m.initialize(this.buffer);
            }
        }
        this.dynamicSplitFraction = previousDynamicSplitFraction;
        resetModelTypeIndex();
    }

    private void resetModelTypeIndex() {
        //Restarts ingestion using the first model type and the currently buffered data points
        this.modelTypeIndex = 0;
        this.currentModelType = modelTypes[modelTypeIndex];
        this.currentModelType.initialize(this.buffer);
    }

    private void emitTemporarySegment() {
        //The fallback model type is used if the current model type cannot represent the data points in the buffer
        ModelType modelTypeToBeEmitted = this.currentModelType;
        if (modelTypeToBeEmitted.length() < this.buffer.size() ||
                Float.isNaN(compressionRatio(modelTypeToBeEmitted))) {
            modelTypeToBeEmitted = this.fallbackModelType;
            modelTypeToBeEmitted.initialize(this.buffer);
        }

        //The list of gaps are copied to ensure they do not change
        ArrayList<Integer> gapsAndPermGaps = new ArrayList<>(this.gaps);
        gapsAndPermGaps.addAll(this.permanentGapTids);

        //A segment containing the current model type is constructed and emitted
        emitSegment(this.temporarySegmentStream, modelTypeToBeEmitted, gapsAndPermGaps);

        //DEBUG: all the debug counters can be updated as we have emitted a temporary segment
        this.logger.updateTemporarySegmentCounters(modelTypeToBeEmitted, gapsAndPermGaps.size());
    }

    private void emitFinalSegment() {
        //The model type providing the model with best compression ratio is selected as mostEfficientModelType
        ModelType mostEfficientModelType = this.modelTypes[0];
        for (ModelType modelType : this.modelTypes) {
            mostEfficientModelType = (compressionRatio(modelType) < compressionRatio(mostEfficientModelType)) ? mostEfficientModelType : modelType;
        }

        //If none of the model types has received enough data points to fit a model to them, the fallback model type is used
        int mostEfficientModelTypeLength = mostEfficientModelType.length();
        float highestCompressionRatio = compressionRatio(mostEfficientModelType);
        if (Float.isNaN(highestCompressionRatio) || mostEfficientModelTypeLength == 0) {
            mostEfficientModelType = this.fallbackModelType;
            mostEfficientModelType.initialize(this.buffer);
            mostEfficientModelTypeLength = mostEfficientModelType.length();
            highestCompressionRatio = compressionRatio(mostEfficientModelType);
        }

        //A segment containing the model with the best compression ratio is constructed and emitted
        List<Integer> gapsAndPermGaps = new ArrayList<>(this.gaps);
        gapsAndPermGaps.addAll(this.permanentGapTids);
        emitSegment(this.finalizedSegmentStream, mostEfficientModelType, gapsAndPermGaps);
        this.buffer.subList(0, mostEfficientModelTypeLength).clear();

        //If the number of data points in the buffer is less then the number of data points that has yet to be
        // emitted, then some of these data points have already been emitted as part of the finalized segment
        this.slicesNotYetEmitted = Math.min(this.slicesNotYetEmitted, buffer.size());

        //The best model is stored as it's error function is used when computing the split/join heuristics
        this.lastEmittedModelType = mostEfficientModelType;

        //DEBUG: all the debug counters are updated based on the emitted finalized segment
        this.logger.updateFinalizedSegmentCounters(mostEfficientModelType, this.gaps.size());

        //If the time series have changed it might beneficial to split or join their groups
        checkIfSplitOrJoinMakesSense(highestCompressionRatio);
    }

    private void checkIfSplitOrJoinMakesSense(float highestCompressionRatio) {
        boolean compressionRatioIsBelowAverage = checkIfCompressionRatioIsBelowAverageAndUpdateTheAverage(highestCompressionRatio);
        if (!this.buffer.isEmpty() && this.buffer.get(0).length > 1 && compressionRatioIsBelowAverage) {
            splitGroupIfItsTimeSeriesAreNoLongerCorrelated();
        } else if (!this.splitSegmentGenerators.isEmpty() && this.emittedFinalizedSegments == this.finalizedSegmentsBeforeNextJoinCheck) {
            this.splitsToJoinIfCorrelated.add(this);
            this.emittedFinalizedSegments = 0;
            this.finalizedSegmentsBeforeNextJoinCheck *= 2;
        }
    }

    private float compressionRatio(ModelType modelType) {
        int modelTypeLength = modelType.length();
        if (modelTypeLength == 0) {
            return Float.NaN;
        }
        long startTime = this.buffer.get(0)[0].timestamp;
        long endTime = this.buffer.get(modelTypeLength - 1)[0].timestamp;
        return modelType.compressionRatio(startTime, endTime, samplingInterval, this.buffer, this.gaps.size());
    }

    private void emitSegment(SegmentFunction stream, ModelType modelType, List<Integer> segmentGaps) {
        if(slicesNotYetEmitted == 0) {
            return;
        }
        int amtPointsInModel = modelType.length();
        long startTime = this.buffer.get(0)[0].timestamp;
        long endTime = this.buffer.get(amtPointsInModel - 1)[0].timestamp;
        int[] gaps = segmentGaps.stream().mapToInt(l -> l).toArray();
        byte[] model = modelType.getModel(startTime, endTime, samplingInterval, this.buffer);
        stream.emit(this.gid, startTime, this.samplingInterval, endTime, modelType.mtid, model, Static.intToBytes(gaps));
    }

    private boolean checkIfCompressionRatioIsBelowAverageAndUpdateTheAverage(double compressionRatio) {
        boolean isBelowAverage = compressionRatio < this.dynamicSplitFraction * this.compressionRatioAverage;
        this.compressionRatioAverage = (this.compressionRatioAverage * this.emittedFinalizedSegments + compressionRatio) / (this.emittedFinalizedSegments + 1);
        this.emittedFinalizedSegments += 1;
        return isBelowAverage;
    }

    private void splitGroupIfItsTimeSeriesAreNoLongerCorrelated() {
        //If only a subset of the time series in it are currently correlated the group is temporarily split into multiple groups
        ValueDataPoint[] bufferHead = this.buffer.get(0);
        float doubleErrorBound = 2 * this.fallbackModelType.errorBound;
        Set<Integer> timeSeriesWithoutGaps = IntStream.range(0, bufferHead.length).boxed().collect(Collectors.toSet());
        timeSeriesWithoutGaps = timeSeriesWithoutGaps.stream().filter(tsIndex -> !bufferHead[tsIndex].isGapPoint()).collect(Collectors.toSet());
        int amountOfTSNotCurrentlyInGap = timeSeriesWithoutGaps.size();

        while (!timeSeriesWithoutGaps.isEmpty()) {
            int i = timeSeriesWithoutGaps.iterator().next();
            ArrayList<Integer> bufferSplitIndexes = new ArrayList<>();
            ArrayList<Integer> timeSeriesSplitIndexes = new ArrayList<>();

            for (Integer j : timeSeriesWithoutGaps) {
                //Comparing a time series to itself should always return true
                if (i == j) {
                    bufferSplitIndexes.add(i);
                    timeSeriesSplitIndexes.add(Collections.binarySearch(tids, bufferHead[i].getTid()));
                    continue;
                }

                //The splitIfNotCorrelated method is only executed if the buffer contains data points
                boolean allDataPointsWithinDoubleErrorBound = lastEmittedModelType.withinErrorBound(doubleErrorBound,
                        this.buffer.stream().map(dps -> dps[i]).iterator(),
                        this.buffer.stream().map(dps -> dps[j]).iterator());

                //Time series should be ingested together if all of their data point are within the double error bound
                if (allDataPointsWithinDoubleErrorBound) {
                    bufferSplitIndexes.add(j);
                    timeSeriesSplitIndexes.add(Collections.binarySearch(tids, bufferHead[j].getTid()));
                }
            }
            //If the size of the split is the number of the time series not currently in a gap, no split is required
            if (bufferSplitIndexes.size() == amountOfTSNotCurrentlyInGap) {
                return;
            }

            //Only the time series that currently are not in a gap can be grouped together as they have data points buffered
            bufferSplitIndexes.forEach(timeSeriesWithoutGaps::remove);
            int[] bufferSplitIndex = bufferSplitIndexes.stream().mapToInt(k -> k).toArray();
            int[] timeSeriesSplitIndex = timeSeriesSplitIndexes.stream().mapToInt(k -> k).toArray();
            splitSegmentGenerator(bufferSplitIndex, timeSeriesSplitIndex);
        }

        //If the number of time series with data points in the buffer is smaller than the size of the group, then some
        // of the time series in the group are in a gap and are grouped together as we have no knowledge about them
        if (amountOfTSNotCurrentlyInGap != tids.size()) {
            int[] timeSeriesSplitIndex = //If a gap's tid is not in this group it is part of another split
                    this.gaps.stream().mapToInt(tid -> Collections.binarySearch(tids, tid)).filter(k -> k >= 0).toArray();
            Arrays.sort(timeSeriesSplitIndex); //This.gaps is a set so sorting is required
            //TODO(EKN): fix bufferSplitINDEX
            splitSegmentGenerator(new int[0], timeSeriesSplitIndex);
        }
        // Sorting that is necessary for testing
        this.splitSegmentGenerators.sort(Comparator.comparingInt(sg -> sg.tids.get(0)));
        this.buffer.clear();
    }

    private void splitSegmentGenerator(int[] bufferSplitIndex, int[] timeSeriesSplitIndexes) {
        ArrayList<Integer> tidsForChild = new ArrayList<>();
        for (int timeSeriesSplitIndex : timeSeriesSplitIndexes) {
            tidsForChild.add(this.tids.get(timeSeriesSplitIndex));
        }
        // We figure out which TIDs the child should not handle
        HashSet<Integer> permGapsForChild = new HashSet<>(this.tids);
        permGapsForChild.removeAll(tidsForChild);

        permGapsForChild.addAll(this.permanentGapTids);

        SegmentGenerator sg = new SegmentGenerator(this.gid, this.samplingInterval, permGapsForChild, this.modelTypeInitializer, this.fallbackModelType,
                tidsForChild, this.maximumLatency, this.dynamicSplitFraction, this.temporarySegmentStream, this.finalizedSegmentStream);
        sg.buffer = copyBuffer(this.buffer, bufferSplitIndex);
        sg.logger = this.logger;
        sg.resetModelTypeIndex();
        sg.splitSegmentGenerators = this.splitSegmentGenerators;
        sg.splitsToJoinIfCorrelated = this.splitsToJoinIfCorrelated;
        int index = this.splitSegmentGenerators.indexOf(this);
        if (index != -1) {
            this.splitSegmentGenerators.set(index, null);
        }
        this.splitSegmentGenerators.add(sg);

        //As the current temporary segment is shared with the parent SegmentGenerator, a new temporary segment is
        // emitted for each split generator so the temporary segment can be updated separately for each generator
        if (this.maximumLatency > 0) {
            this.slicesNotYetEmitted = 0;
            sg.emitTemporarySegment();
        }
    }

    private ArrayList<ValueDataPoint[]> copyBuffer(ArrayList<ValueDataPoint[]> buffer, int[] bufferSplitIndex) {
        //No data points are buffered for time series currently in a gap
        if (bufferSplitIndex.length == 0) {
            return new ArrayList<>();
        }

        //Copies all data points for the split time series to the new buffer
        ArrayList<ValueDataPoint[]> newBuffer = new ArrayList<>(buffer.size());
        for (ValueDataPoint[] slice : buffer) {
            ValueDataPoint[] newSlice = new ValueDataPoint[bufferSplitIndex.length];
            int j = 0;
            for (int i : bufferSplitIndex) {
                newSlice[j] = new ValueDataPoint(slice[i].getTid(), slice[i].timestamp, slice[i].value, samplingInterval);
                j++;
            }
            newBuffer.add(newSlice);
        }
        return newBuffer;
    }

    private void joinGroupsIfTheirTimeSeriesAreCorrelated() {
        //Assumes that time series which are not correlated would have been split of from the group, so only [0] is checked
        float doubleErrorBound = 2 * this.fallbackModelType.errorBound;
        HashSet<SegmentGenerator> markedForJoining = new HashSet<>();
        ArrayList<SegmentGenerator> joined = new ArrayList<>();
        // Double loop where we try to join each segment generator with the other segment generators
        while (!this.splitsToJoinIfCorrelated.isEmpty()) {
            SegmentGenerator sgi = this.splitsToJoinIfCorrelated.iterator().next();
            HashSet<SegmentGenerator> toBeJoined = new HashSet<>();
            // TODO MOVE THIS INTO A HELPER FUNCTION
            //If all data points with a shared time stamp is within the double error bound the groups are joined
            for (SegmentGenerator sgj : this.splitSegmentGenerators) {
                //Comparing the segment generator to itself always return true
                if (sgi == sgj) {
                    toBeJoined.add(sgi);
                    markedForJoining.add(sgi);
                    this.splitsToJoinIfCorrelated.remove(sgi);
                    continue;
                }

                //A time series group cannot be joined with another group more than once
                if (markedForJoining.contains(sgj)) {
                    continue;
                }

                //If no data points are buffered it is not possible to check if the time series should be joined
                int is = sgi.buffer.size();
                int js = sgj.buffer.size();
                boolean canBeJoined = is > 0 && js > 0 &&
                        sgi.buffer.get(is - 1)[0].timestamp == sgj.buffer.get(js - 1)[0].timestamp;

                //The time series are joined if their data points with equal time stamps are within twice the error bound
                canBeJoined &= lastEmittedModelType.withinErrorBound(doubleErrorBound,
                        new ReverseBufferIterator(sgi.buffer, 0), new ReverseBufferIterator(sgj.buffer, 0));

                if (canBeJoined) {
                    toBeJoined.add(sgj);
                    markedForJoining.add(sgj);
                    this.splitsToJoinIfCorrelated.remove(sgj);
                }
            }

            //If the join set contains more than one SegmentGenerator they are joined together
            if (toBeJoined.size() > 1) {
                joinSegmentGenerators(toBeJoined, joined);
                //HACK: a SegmentGenerator might add itself to the splitsToJoinIfCorrelated list while being joined
                this.splitsToJoinIfCorrelated.removeAll(toBeJoined);
            }
        }
        this.splitSegmentGenerators.addAll(joined);
    }

    private void joinSegmentGenerators(Set<SegmentGenerator> sgs, ArrayList<SegmentGenerator> joined) {
        //The join index is build with the assumption that groups are numerically ordered by tid
        ArrayList<Integer> totalJoinIndexList = new ArrayList<>();
        ArrayList<Integer> activeJoinIndexList = new ArrayList<>();

        for (SegmentGenerator sg : sgs) {
            totalJoinIndexList.addAll(sg.tids);
            for (Integer tid : sg.tids) {
                //Segment generators store the tid for all time series it controls currently in a gap
                if (!sg.gaps.contains(tid)) {
                    activeJoinIndexList.add(tid);
                }
            }
        }

        //If the original group is recreated the master SegmentGenerator is used, otherwise a new one is created
        SegmentGenerator newSegmentGenerator;
        if (this.tids.size() == totalJoinIndexList.size()) {
            newSegmentGenerator = this;
        } else {
            Set<Integer> allSgPermanentGaps = sgs.stream().flatMap(sg -> sg.permanentGapTids.stream()).collect(Collectors.toSet());
            totalJoinIndexList.forEach(allSgPermanentGaps::remove);

            newSegmentGenerator = new SegmentGenerator(this.gid, this.samplingInterval, allSgPermanentGaps, this.modelTypeInitializer, this.fallbackModelType, totalJoinIndexList,
                    this.maximumLatency, this.dynamicSplitFraction, this.temporarySegmentStream, this.finalizedSegmentStream);
            newSegmentGenerator.logger = this.logger;
            newSegmentGenerator.splitSegmentGenerators = this.splitSegmentGenerators;
            newSegmentGenerator.splitsToJoinIfCorrelated = this.splitsToJoinIfCorrelated;
            joined.add(newSegmentGenerator);
        }
        this.splitSegmentGenerators.removeAll(sgs);

        newSegmentGenerator.gaps = new HashSet<>();
        for (SegmentGenerator sg : sgs) {
            //The remaining data points stored by each SegmentGenerator are flushed
            sg.flushBuffer(); // TODO: more testing needed if we can remove this instead and move the buffer down
            //add sub gap lists to gaps
            newSegmentGenerator.gaps.addAll(sg.gaps);
        }

        //Initializes the first model with the content in the new combined buffer
        newSegmentGenerator.resetModelTypeIndex();

        //As multiple temporary segments currently represent values for the new combined group, a new temporary segment
        // is emitted so the existing temporary segments can be overwritten by one temporary segment from nsg
        if (this.maximumLatency > 0) {
            newSegmentGenerator.emitTemporarySegment();
        }
    }
}