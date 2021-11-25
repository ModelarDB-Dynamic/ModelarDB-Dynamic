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

