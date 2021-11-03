package dk.aau.modelardb.core.Models;

import java.util.Objects;

public abstract class DataPoint {
    private final int tid;

    public DataPoint(int tid) {
        this.tid = tid;
    }

    public int getTid() {
        return tid;
    }

    public abstract boolean isConfigurationDataPoint();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataPoint dataPoint = (DataPoint) o;
        return tid == dataPoint.tid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tid);
    }
}
