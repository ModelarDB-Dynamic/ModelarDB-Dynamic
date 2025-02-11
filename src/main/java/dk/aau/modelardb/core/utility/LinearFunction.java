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
package dk.aau.modelardb.core.utility;

public class LinearFunction {

    /**
     * Instance Variables
     **/
    public final double a, b;

    /**
     * Constructors
     **/
    public LinearFunction(long ts, double vs, long te, double ve) {
        this.a = (ve - vs) / (te - ts);
        this.b = vs - a * ts;
    }

    /**
     * Public Methods
     **/
    public double get(long ts) {
        return this.a * ts + this.b;
    }
}
