package dk.aau.modelardb.core.GroupBasedCompression;

import dk.aau.modelardb.core.Models.CompressionModels.ModelType;
import dk.aau.modelardb.core.Models.DataSlice;
import dk.aau.modelardb.core.utility.SegmentFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class SegmentGeneratorController {
    private TimeSeriesGroup timeSeriesGroup;
    private Map<Integer, SegmentGenerator> samplingIntervalToSegmentGenerator;

    /**
     * Fields from WorkingSet used to instantiate SegmentGenerator
     */
    private final Supplier<ModelType[]> modelTypeInitializer;
    private final ModelType fallbackModelType;
    private List<Integer> tids;
    private final int maximumLatency;
    private float dynamicSplitFraction;
    private final SegmentFunction temporarySegmentStream;
    private final SegmentFunction finalizedSegmentStream;

    public SegmentGeneratorController(TimeSeriesGroup timeSeriesGroup, Supplier<ModelType[]> modelTypeInitializer,
                                      ModelType fallbackModelType, List<Integer> tids, int maximumLatency, float dynamicSplitFraction,
                                      SegmentFunction temporarySegmentStream, SegmentFunction finalizedSegmentStream) {
        this.timeSeriesGroup = timeSeriesGroup;
        this.samplingIntervalToSegmentGenerator = new HashMap<>();
        this.modelTypeInitializer = modelTypeInitializer;
        this.fallbackModelType = fallbackModelType;
        this.tids = tids;
        this.maximumLatency = maximumLatency;
        this.dynamicSplitFraction = dynamicSplitFraction;
        this.temporarySegmentStream = temporarySegmentStream;
        this.finalizedSegmentStream = finalizedSegmentStream;
    }
    
    public void start(){
        while (timeSeriesGroup.hasNext()){
            delegateSliceToSegmentGenerators(timeSeriesGroup.GetSlice());
        }
    }
    
    private void delegateSliceToSegmentGenerators(DataSlice slice){
        //TODO TIDS FOR SLICE GIVEN SAMPLING INTERVAL MUST BE CHECKED AS WELL AS SI
        if (samplingIntervalToSegmentGenerator.containsKey(slice.samplingInterval)) {
            samplingIntervalToSegmentGenerator.get(slice.samplingInterval).consumeSlice(slice);
        } else {
            SegmentGenerator sg = new SegmentGenerator(this.timeSeriesGroup,
                    modelTypeInitializer, fallbackModelType, tids, maximumLatency, dynamicSplitFraction, temporarySegmentStream,
                    finalizedSegmentStream);
            samplingIntervalToSegmentGenerator.put(slice.samplingInterval, sg);

            sg.consumeSlice(slice);
        }
    }
    
    
}
