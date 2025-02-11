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

import dk.aau.modelardb.core.DataPoint;

import java.util.ArrayList;
import java.util.Iterator;

public class ReverseBufferIterator implements Iterator<DataPoint> {

    /**
     * Instance Variables
     **/
    private final int source;
    private final ArrayList<DataPoint[]> list;
    private int index;

    /**
     * Constructors
     **/
    public ReverseBufferIterator(ArrayList<DataPoint[]> list, int source) {
        this.index = list.size();
        this.list = list;
        this.source = source;
    }

    @Override
    public boolean hasNext() {
        return this.index > 0;
    }

    @Override
    public DataPoint next() {
        this.index -= 1;
        return this.list.get(this.index)[source];
    }
}
