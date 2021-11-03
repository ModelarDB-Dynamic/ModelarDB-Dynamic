package dk.aau.modelardb.core.Models;

import scala.Int;

import java.util.regex.Pattern;

public class SIConfigurationDataPoint extends DataPoint {

    private final String configurationKey;
    private final int newSamplingInterval;
    private final int previousSamplingInterval;

    public SIConfigurationDataPoint(int timeSeriesId, int newSamplingInterval, int previousSamplingInterval) {
        super(timeSeriesId);
        this.configurationKey = "SI";
        this.newSamplingInterval = newSamplingInterval;
        this.previousSamplingInterval = previousSamplingInterval;
    }


    public static boolean isAConfigurationDataPoint(String timeSeriesKey) {
        // OMEGAHACK
        return Pattern.matches("[^0-9]", timeSeriesKey);
    }

    public String getConfigurationKey() {
        return configurationKey;
    }

    public int getNewSamplingInterval() {
        return newSamplingInterval;
    }

    public int getPreviousSamplingInterval() {
        return previousSamplingInterval;
    }

    public boolean hasPreviousSamplingInterval() {
        return previousSamplingInterval != Integer.MIN_VALUE;
    }

    public String toString() {
        String previousSIstring;
        if (!hasPreviousSamplingInterval()) {
            previousSIstring = "No previous SI";
        } else {
            previousSIstring = Integer.toString(previousSamplingInterval);
        }

        return "SIConfigurationDataPoint: [" + this.getTid() + " | " + newSamplingInterval + " | " + previousSIstring + "]";
    }

    @Override
    public boolean isConfigurationDataPoint() {
        return true;
    }
}
