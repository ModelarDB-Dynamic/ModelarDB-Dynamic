package dk.aau.modelardb.core.Models;

public abstract class DataPoint {
    private final int tid;

    public DataPoint(int tid) {
        this.tid = tid;
    }

    public int getTid() {
        return tid;
    }

    abstract boolean isConfigurationDataPoint();

}
