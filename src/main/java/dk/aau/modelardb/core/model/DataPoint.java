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

package dk.aau.modelardb.core.model;

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
