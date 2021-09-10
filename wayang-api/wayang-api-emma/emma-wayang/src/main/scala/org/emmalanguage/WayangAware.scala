/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
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
package org.emmalanguage

import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

trait WayangAware {

  protected trait WayangConfig

  protected val defaultWayangConfig = new WayangConfig {}

  protected lazy val defaultWayangContext =
    wayangEnv(defaultWayangConfig)

  protected def wayangEnv(c: WayangConfig): WayangContext = {
    new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)
  }

  protected def withDefaultWayangEnv[T](f: WayangContext => T): T =
    f(defaultWayangContext)
}
