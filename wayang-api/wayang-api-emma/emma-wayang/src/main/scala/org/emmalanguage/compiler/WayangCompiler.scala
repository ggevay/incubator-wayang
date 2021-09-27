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
package compiler

import compiler.backend.WayangBackend
import compiler.opt.WayangOptimizations
import cats.implicits._
import com.typesafe.config.Config

trait WayangCompiler extends Compiler
  with WayangBackend
  with WayangOptimizations {

  override val baseConfig = "reference.emma.onWayang.conf" +: super.baseConfig

  override lazy val implicitTypes: Set[u.Type] = API.implicitTypes ++ WayangAPI.implicitTypes

  import UniverseImplicits._

  def transformations(implicit cfg: Config): Seq[TreeTransform] = Seq(
    // lifting
    Lib.expand,
    Core.lift,
    // optimizations
    Core.cse iff "emma.compiler.opt.cse" is true,
    //FlinkOptimizations.specializeLoops iff "emma.compiler.flink.native-its" is true,
    Optimizations.foldFusion iff "emma.compiler.opt.fold-fusion" is true,
    Optimizations.addCacheCalls iff "emma.compiler.opt.auto-cache" is true,
    // backend
    Comprehension.combine,
    Core.unnest,
    WayangBackend.transform,
    // lowering
    Core.trampoline iff "emma.compiler.lower" is "trampoline",
    Core.dscfInv iff "emma.compiler.lower" is "dscfInv",
    removeShadowedThis
  ) filterNot (_ == noop)

  trait NtvAPI extends ModuleAPI {
    //@formatter:off
    val sym               = api.Sym[org.emmalanguage.api.wayang.WayangNtv.type].asModule

    //val iterate           = op("iterate") // when uncommenting this, don't forget to also uncomment in ops below

    val map               = op("map")
    val flatMap           = op("flatMap")
    val filter            = op("filter")

    val broadcast         = op("broadcast")
    val bag               = op("bag")

    //override lazy val ops = Set(iterate, map, flatMap, filter, broadcast, bag)
    override lazy val ops = Set(map, flatMap, filter, broadcast, bag)
    //@formatter:on
  }

  object WayangAPI extends BackendAPI {
    lazy val ExecutionContext = api.Type[org.apache.wayang.core.function.ExecutionContext]
    lazy val WayangContext = api.Type[org.apache.wayang.core.api.WayangContext]

    //lazy val implicitTypes = Set(TypeInformation, ExecutionEnvironment)
    lazy val implicitTypes = Set(WayangContext)

    lazy val State = api.Type[org.emmalanguage.api.WayangMutableBag.State[Any, Any]].typeConstructor

    lazy val DataBag = new DataBagAPI(api.Sym[org.emmalanguage.api.DataQuantaDataBag[Any]].asClass)

    lazy val DataBag$ = new DataBag$API(api.Sym[org.emmalanguage.api.DataQuantaDataBag.type].asModule)

    lazy val MutableBag = new MutableBagAPI(api.Sym[org.emmalanguage.api.WayangMutableBag[Any, Any]].asClass)

    lazy val MutableBag$ = new MutableBag$API(api.Sym[org.emmalanguage.api.WayangMutableBag.type].asModule)

    lazy val Ops = new OpsAPI(api.Sym[org.emmalanguage.api.wayang.WayangOps.type].asModule)

    lazy val Ntv = new NtvAPI {}

    lazy val GenericOps = for {
      ops <- Set(DataBag.ops, DataBag$.ops, MutableBag.ops, MutableBag$.ops, Ops.ops)
      sym <- ops
      if sym.info.takesTypeArgs
      res <- sym +: sym.overrides
    } yield res
  }

}
