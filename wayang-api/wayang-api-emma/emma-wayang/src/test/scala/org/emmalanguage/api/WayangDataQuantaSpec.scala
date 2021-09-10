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
package api

import org.apache.wayang.core.api.WayangContext
import test.schema.Literature._

class WayangDataQuantaSpec extends DataBagSpec with WayangAware {

  override val supportsParquet = false

  override type TestBag[A] = DataQuantaDataBag[A]
  override type BackendContext = WayangContext

  override val TestBag = DataQuantaDataBag
  override val suffix = "wayang"

  override def withBackendContext[T](f: BackendContext => T): T =
    withDefaultWayangEnv(f)

//  "broadcast support" in withBackendContext(implicit env => {
//    val us = TestBag(400 to 410)
//    val vs = TestBag(440 to 450)
//    val ws = TestBag(480 to 490)
//    val xs = TestBag(1 to 50)
//    val fn = (ctx: RuntimeContext) => {
//      val us1 = wayang.WayangNtv.bag(us)(ctx)
//      val vs1 = wayang.WayangNtv.bag(vs)(ctx)
//      val ws1 = wayang.WayangNtv.bag(ws)(ctx)
//      (y: Int) => (us1 union vs1 union ws1).exists(_ == y * y)
//    }
//    val rs = wayang.WayangNtv.filter(fn)(xs)
//    val b1 = wayang.WayangNtv.broadcast(rs, us)
//    val b2 = wayang.WayangNtv.broadcast(b1, vs)
//    val b3 = wayang.WayangNtv.broadcast(b2, ws)
//    b3.collect() should contain theSameElementsAs Seq(20, 21, 22)
//  })
}
