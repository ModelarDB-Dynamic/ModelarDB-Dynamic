package MockData;

import dk.aau.modelardb.core.Configuration;

// This class updates the Configuration.INSTANCE object with some predefined values
public class ConfigurationProvider {
    private final static int samplingInterval = 100;

    public static void setDefaultValuesForConfigurationInstance() {
        Configuration.INSTANCE.add("modelardb.sampling_interval", samplingInterval);
    }

    public static void removeDefaultValues() {
        Configuration.INSTANCE.remove("modelardb.sampling_interval");
    }
}
