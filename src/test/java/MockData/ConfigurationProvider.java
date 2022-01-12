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
