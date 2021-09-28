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

import org.apache.wayang.api.DataQuanta

import _root_.java.lang.{Iterable => JavaIterable}
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.function.FunctionDescriptor.{ExtendedSerializableFunction, ExtendedSerializablePredicate}
import org.emmalanguage.api._

import scala.collection.JavaConverters

object WayangNtv {

  type R = ExecutionContext

  import Meta.Projections.ctagFor

  //----------------------------------------------------------------------------
  // Loops
  //----------------------------------------------------------------------------

  def repeat[A: Meta](xs: DataBag[A])(
    N: Int, body: DataBag[A] => DataBag[A]
  )(
    implicit wayang: WayangContext
  ): DataBag[A] = xs match {
    case DataQuantaDataBag(us) => DataQuantaDataBag(us.repeat(N, unlift(body)))
  }

  //----------------------------------------------------------------------------
  // Broadcast support
  //----------------------------------------------------------------------------

  def broadcast[A: Meta, B: Meta](xs: DataBag[A], ys: DataBag[B])(
    implicit wayang: WayangContext
  ): DataBag[A] = (xs, ys) match {
    case (DataQuantaDataBag(us), DataQuantaDataBag(vs)) =>
      us.withBroadcast(vs, ys.uuid.toString)
      xs
  }

  def bag[A: Meta](xs: DataBag[A])(ctx: R): DataBag[A] = {
    DataBag(ctx.getBroadcast[A](xs.uuid.toString))
  }

  def map[A: Meta, B: Meta](h: R => A => B)(xs: DataBag[A])(
    implicit wayang: WayangContext
  ): DataBag[B] = xs match {
    case DataQuantaDataBag(us) => DataQuantaDataBag(us.mapJava(new ExtendedSerializableFunction[A, B] {
      var f: A => B = _

      override def open(executionCtx: ExecutionContext): Unit =
        f = h(executionCtx)

      override def apply(x: A): B =
        f(x)
    }))
  }

  def flatMap[A: Meta, B: Meta](h: R => A => DataBag[B])(xs: DataBag[A])(
    implicit wayang: WayangContext
  ): DataBag[B] = xs match {
    case DataQuantaDataBag(us) => DataQuantaDataBag(us.flatMapJava(new ExtendedSerializableFunction[A, JavaIterable[B]] {
      var f: A => DataBag[B] = _

      override def open(executionCtx: ExecutionContext): Unit =
        f = h(executionCtx)

      override def apply(x: A): JavaIterable[B] = {
        JavaConverters.asJavaIterableConverter(f(x).collect()).asJava
      }
    }))
  }

  def filter[A: Meta](h: R => A => Boolean)(xs: DataBag[A])(
    implicit wayang: WayangContext
  ): DataBag[A] = xs match {
    case DataQuantaDataBag(us) => DataQuantaDataBag(us.filterJava(new ExtendedSerializablePredicate[A] {
      var p: A => Boolean = _

      override def open(executionCtx: ExecutionContext): Unit =
        p = h(executionCtx)

      override def test(x: A): Boolean =
        p(x)
    }))
  }

  //----------------------------------------------------------------------------
  // Helper Objects and Methods
  //----------------------------------------------------------------------------

  private def unlift[A: Meta](f: DataBag[A] => DataBag[A])(
    implicit wayang: WayangContext
  ): DataQuanta[A] => DataQuanta[A] = xs => f(DataQuantaDataBag(xs)) match {
    case DataQuantaDataBag(ys) => ys
  }
}
