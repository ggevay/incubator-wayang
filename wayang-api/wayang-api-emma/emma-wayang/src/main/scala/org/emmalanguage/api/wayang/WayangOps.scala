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
package api.wayang

import api._
import api.alg._
import api.backend.ComprehensionCombinators
import api.backend.Runtime
import org.apache.wayang.api.DataQuanta
import org.apache.wayang.core.api.WayangContext

import java.net.URI

/** Wayang backend operators. These are DataBag opreators, but not part of the public DataBag API,
 *  because the Emma compiler introduces them (in comprehension combination, fold-group fusion, etc.)
 */
object WayangOps extends ComprehensionCombinators[WayangContext] with Runtime[WayangContext] {

  import Meta.Projections._

  // ---------------------------------------------------------------------------
  // ComprehensionCombinators
  // ---------------------------------------------------------------------------

  def cross[A: Meta, B: Meta](
    xs: DataBag[A], ys: DataBag[B]
  )(implicit wayang: WayangContext): DataBag[(A, B)] = (xs, ys) match {
    case (DataQuantaDataBag(us), DataQuantaDataBag(vs)) => DataQuantaDataBag(
      (us cartesian vs)
        .map(wayangTuple2ToScalaTuple2))
  }

  def equiJoin[A: Meta, B: Meta, K: Meta](
    kx: A => K, ky: B => K)(xs: DataBag[A], ys: DataBag[B]
  )(implicit wayang: WayangContext): DataBag[(A, B)] = (xs, ys) match {
    case (DataQuantaDataBag(us), DataQuantaDataBag(vs)) => DataQuantaDataBag(
      (us.keyBy(kx) join vs.keyBy(ky))
        .map(wayangTuple2ToScalaTuple2))
  }

  // ---------------------------------------------------------------------------
  // Runtime
  // ---------------------------------------------------------------------------

  def cache[A: Meta](xs: DataBag[A])(implicit wayang: WayangContext): DataBag[A] =
    xs match {
      case xs: DataQuantaDataBag[A] =>
        val sinkName = sink(xs.rep)
        //xs.env.execute(s"emma-cache-$sinkName")
        DataQuantaDataBag(source[A](sinkName))
      case _ => xs
    }

  def foldGroup[A: Meta, B: Meta, K: Meta](
    xs: DataBag[A], key: A => K, alg: Alg[A, B]
  )(implicit wayang: WayangContext): DataBag[Group[K, B]] = xs match {
    case DataQuantaDataBag(us) => DataQuantaDataBag(us
      .map(x => key(x) -> alg.init(x))
      .reduceByKey(_._1, (kv1,kv2) => (kv1._1, alg.plus(kv1._2,kv2._2)))
      .map(x => Group(x._1, x._2)))
  }

  private def sink[A: Meta](xs: DataQuanta[A])(implicit wayang: WayangContext): String = {
//    val tempName = tempNames.next()
//    xs.writeTextFile(tempPath(tempName), serialize it with e.g. chill)
//    tempName
    ??? // TODO: Needed for caching
  }

  private def source[A: Meta](fileName: String)(implicit wayang: WayangContext): DataQuanta[A] = {
    //val filePath = tempPath(fileName)
    // read it back, and deserialize
    ??? // TODO: Needed for caching
  }

  lazy val tempBase =
    new URI(System.getProperty("emma.wayang.temp-base", "file:///tmp/emma/wayang-temp/"))

  private[emmalanguage] val tempNames = Stream.iterate(0)(_ + 1)
    .map(i => f"dataflow$i%03d")
    .toIterator

  private[emmalanguage] def tempPath(tempName: String): String =
    tempBase.resolve(tempName).toString

  def wayangTuple2ToScalaTuple2[A,B](wt: org.apache.wayang.basic.data.Tuple2[A,B]): (A, B) = (wt.field0, wt.field1)
}
