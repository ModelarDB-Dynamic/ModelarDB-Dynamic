package dk.aau.modelardb.core.Models;

import java.util.regex.Pattern;

public class SIConfigurationDataPoint extends DataPoint {

    private final String configurationKey;
    private final int samplingInterval;

    public SIConfigurationDataPoint(int timeSeriesId, String configurationKey, int samplingInterval) {
        super(timeSeriesId);
        this.configurationKey = configurationKey;
        this.samplingInterval = samplingInterval;
    }

    public static boolean isAConfigurationDataPoint(String timeSeriesKey) {
        return Pattern.matches("[^0-9]", timeSeriesKey);
    }

    public String getConfigurationKey() {
        return configurationKey;
    }

    public int getSamplingInterval() {
        return samplingInterval;
    }

    @Override
    public boolean isConfigurationDataPoint() {
        return true;
    }
}
