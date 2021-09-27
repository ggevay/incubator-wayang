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

import alg.Alg
import org.apache.wayang.api.DataQuanta
import org.apache.wayang.core.api.WayangContext

import scala.util.hashing.MurmurHash3
import org.apache.wayang.api._
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval

import scala.collection.JavaConversions.iterableAsScalaIterable

/** A `DataBag` implementation backed by a Wayang `DataQuanta`. */
class DataQuantaDataBag[A: Meta] private[api]
(
  @transient private[emmalanguage] val rep: DataQuanta[A]
) extends DataBag[A] {

  //import DataQuantaDataBag.typeInfoForType
  import Meta.Projections._

  @transient override val m: Meta[A] = implicitly[Meta[A]]

  private[emmalanguage] implicit def env: WayangContext = this.rep.planBuilder.wayangContext

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](alg: Alg[A, B]): B = {
    val collected = rep.map(x => alg.init(x)).reduce(alg.plus).collect()
    assert(collected.size <= 1)
    if (collected.isEmpty) alg.zero
    else collected.head
  }

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: A => B): DataBag[B] =
    DataQuantaDataBag(rep.map(f))

  override def flatMap[B: Meta](f: A => DataBag[B]): DataBag[B] =
    DataQuantaDataBag(rep.flatMap((x: A) => f(x).collect()))

  def withFilter(p: A => Boolean): DataBag[A] =
    DataQuantaDataBag(rep.filter(p))

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: A => K): DataBag[Group[K, DataBag[A]]] = {
    DataQuantaDataBag(rep.groupByKey(k).map(it => {
      // The temporary buffer comes from the Flink implementation, because the iterator there might not be serializable.
      // TODO: We should try if it works without this here and/or look into what is the Iterable that Wayang gives us.
      val buffer = it.toBuffer
      Group(k(buffer.head), DataBag(buffer))
    }))
  }

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case dbag: SeqDataBag[A] => this union DataQuantaDataBag(dbag.rep)
    case dbag: DataQuantaDataBag[A] => DataQuantaDataBag(this.rep union dbag.rep)
    case _ => throw new IllegalArgumentException(s"Unsupported rhs for `union` of type: ${that.getClass}")
  }

  override def distinct: DataBag[A] =
    DataQuantaDataBag(rep.distinct)

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit = {
    rep.mapPartitions((it: Iterable[A]) => new Traversable[String] {
      def foreach[U](f: (String) => U): Unit = {
        val csv = CSVScalaSupport[A](format).writer()
        val con = CSVConverter[A]
        val rec = Array.ofDim[String](con.size)
        for (x <- it) {
          con.write(x, rec, 0)(format)
          f(csv.writeRowToString(rec))
        }
      }
    }.toIterable).writeTextFile(path, Predef.identity)
  }

  override def writeText(path: String): Unit =
    rep.writeTextFile(path, _.toString)

  override def collect(): Seq[A] = collected

  private lazy val collected: Seq[A] =
    rep.collect().toSeq

  // -----------------------------------------------------
  // equals, hashCode and toString
  // -----------------------------------------------------

  override def equals(o: Any) =
    super.equals(o)

  override def hashCode(): Int = {
    val (a, b, c, n) = rep
      .mapPartitions(it => {
        var a, b, n = 0
        var c = 1
        it foreach { x =>
          val h = x.##
          a += h
          b ^= h
          if (h != 0) c *= h
          n += 1
        }
        Some((a, b, c, n)).toIterable
      })
      .collect()
      .fold((0, 0, 1, 0))((x, r) => (x, r) match {
        case ((a1, b1, c1, n1), (a2, b2, c2, n2)) => (a1 + a2, b1 ^ b2, c1 * c2, n1 + n2)
      })

    var h = MurmurHash3.traversableSeed
    h = MurmurHash3.mix(h, a)
    h = MurmurHash3.mix(h, b)
    h = MurmurHash3.mixLast(h, c)
    MurmurHash3.finalizeHash(h, n)
  }
}

object DataQuantaDataBag extends DataBagCompanion[WayangContext] {

  import Meta.Projections._

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A: Meta](
    implicit wayang: WayangContext
  ): DataBag[A] = apply(Seq.empty)

  def apply[A: Meta](values: Seq[A])(
    implicit wayang: WayangContext
  ): DataBag[A] = DataQuantaDataBag(wayang.loadCollection(values))

  def readText(path: String)(
    implicit wayang: WayangContext
  ): DataBag[String] = DataQuantaDataBag(wayang.readTextFile(path))

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(
    implicit wayang: WayangContext
  ): DataBag[A] = DataQuantaDataBag(wayang
    .readTextFile(path) // format.charset is ignored for now. (comes from org.apache.spark.sql.DataFrameReader)
    .mapPartitions((it: Iterable[String]) => new Traversable[A] {
      def foreach[U](f: A => U): Unit = {
        val csv = CSVScalaSupport[A](format).parser()
        val con = CSVConverter[A]
        for (line <- it) f(con.read(csv.parseLine(line), 0)(format))
      }
    }.toIterable, ProbabilisticDoubleInterval.ofExactly(1d)))

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  private[api] def apply[A: Meta](
    rep: DataQuanta[A]
  )(implicit wayang: WayangContext): DataBag[A] = new DataQuantaDataBag(rep)

  private[api] def unapply[A: Meta](
    bag: DataBag[A]
  )(implicit wayang: WayangContext): Option[DataQuanta[A]] = bag match {
    case bag: DataQuantaDataBag[A] => Some(bag.rep)
    case _ => Some(wayang.loadCollection(bag.collect()))
  }
}
