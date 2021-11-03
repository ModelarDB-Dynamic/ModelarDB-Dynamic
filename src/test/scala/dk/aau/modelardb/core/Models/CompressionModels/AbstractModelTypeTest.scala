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
package dk.aau.modelardb.core.Models.CompressionModels

import dk.aau.modelardb.core.Models.ValueDataPoint
import org.scalatest.wordspec.AnyWordSpec

import java.util

abstract class AbstractModelTypeTest extends AnyWordSpec {

  "when empty" should {
    val model = getModelType(1, 10, 50)
    val noDataPoints = new util.ArrayList[Array[ValueDataPoint]]()
    model.initialize(noDataPoints)

    "have length zero" in {
      assert(model.length() == 0)
    }

    "have size NaN" in {
      assert(model.unsafeSize().isNaN)
    }
  }

  "when initialized with N data points" should {
    val model = getModelType(1, 10, 50)
    val dataPoints = new util.ArrayList[Array[ValueDataPoint]]()
    dataPoints.add(Array(new ValueDataPoint(1, 100, 25, 100)))
    dataPoints.add(Array(new ValueDataPoint(1, 200, 25, 100)))
    dataPoints.add(Array(new ValueDataPoint(1, 300, 25, 100)))
    dataPoints.add(Array(new ValueDataPoint(1, 400, 25, 100)))
    dataPoints.add(Array(new ValueDataPoint(1, 500, 25, 100)))
    model.initialize(dataPoints)

    "have length N" in {
      assert(model.length() == dataPoints.size())
    }

    "not have size NaN" in {
      assert(!model.unsafeSize().isNaN)
    }

    "not have size +/-Infinity" in {
      assert(!model.unsafeSize().isInfinite)
    }

    "have a positive size" in {
      assert(model.unsafeSize() > 0)
    }
  }

  /** Protected Methods * */
  protected def getModelType(mtid: Int, errorBound: Float, lengthBound: Int): ModelType
}
