package org.emmalanguage
package compiler.lang

import org.emmalanguage._
import org.emmalanguage.api._
import org.emmalanguage.api.DataBag._
import org.emmalanguage.api.alg.Alg
import org.emmalanguage.compiler.Common
import org.emmalanguage.compiler.Compiler
import org.emmalanguage.api.Meta
import shapeless.::

/**
 * Wraps scalars (non-bag values) in singleton (one-element) bags.
 * This is needed, for example, for WayangSpecializeLoops, because in the function that we give to
 * Wayang's `repeat`, everything has to be part of the dataflow job. So everything that was originally a non-bag value
 * has to be a bag. This is similar to what Mitos does in IV.A. 2nd paragraph:
 * https://www.researchgate.net/publication/349768477_Efficient_Control_Flow_in_Dataflow_Systems_When_Ease-of-Use_Meets_High_Performance
 *
 * The implementation is based on MitosNormalization in emma-mitos.
 */
private[compiler] trait WrapScalars extends Common {
  self: Compiler =>

  private[compiler] object WrapScalars {

    import API._
    import UniverseImplicits._

    val core = Core.Lang

    def transform: TreeTransform = TreeTransform("WrapScalars", (tree: u.Tree) => {
      val replacements = scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]()
      val defs = scala.collection.mutable.Map[u.TermSymbol, u.ValDef]()

      // println("==0tree Normalization==")
      // println(tree)
      // println("==0tree END==")

      // first traversal does the actual wrapping, second does block type correction.
      val firstRun = api.TopDown.unsafe
        .withOwner
        .transformWith {

          // find a valdef - according to the rhs we have to do different transformations
          case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _)
            if !meta(vd).all.all.contains(SkipTraversal)
              && refsSeen(rhs, replacements) && !isFun(lhs) && !hasFunInOwnerChain(lhs) && !isAlg(rhs) =>

            // helper function to make sure that arguments in a "fromSingSrc"-method are indeed singSources
            def defcallargBecameSingSrc(s: u.TermSymbol) : Boolean = {
              val argSymRepl = replacements(s)
              val argReplDef = defs(argSymRepl)
              isDataBag(argReplDef.rhs) && meta(argReplDef).all.all.contains(SkipTraversal)
            }

            rhs match {

              // if the rhs of a valdef is simply a valref, replace that valref with the valref of a newly created valdef,
              // iff original valdef has been replaced by a databag
              case core.ValRef(sym) if replacements.keys.toList.contains (sym) =>
                val nvr = core.ValRef(replacements(sym))
                val ns = newValSym(owner, lhs.name.toString, nvr)
                val nvd = core.ValDef(ns, nvr)
                skip(nvd)
                replacements += (lhs -> ns)
                nvd

              /*
              In the following we have to catch all the cases where the user manually creates Databags.

              first: check if the rhs is a singleton bag of Seq[A] due to anf and mitos transformation
              {{{
                   val a = Seq.apply[Int](2)
                   val b = DataBag.apply[Int](a)

                   val a' = singSrc[Seq[Int]](Seq(2)) ==> DataBag[Seq[Int]]
                   val b' = fromSingSrc[Int](a')      ==> DataBag[Int]
              }}}
               */
              case core.DefCall(_, DataBag$.apply, _, Seq(Seq(core.ValRef(argsym)))) =>
                val argSymRepl = replacements(argsym)
                val argReplRef = core.ValRef(argSymRepl)

                if (defcallargBecameSingSrc(argsym)) {
                  val dbRhs = core.DefCall(
                    Some(SingletonBagOps$.ref),
                    SingletonBagOps$.fromSingSrcApply,
                    Seq(
                      argReplRef.tpe.widen // something like DataBag[Seq[A]]
                        .typeArgs.head // e.g., Seq[A], but beware that it can be a descendant, such as Range.Inclusive
                        .baseType(api.Type.seq.typeSymbol) // get Seq[Int] from Range.Inclusive
                        .typeArgs.head),
                    Seq(Seq(argReplRef))
                  )
                  val dbSym = newValSym(owner, "db", dbRhs)
                  val db = core.ValDef(dbSym, dbRhs)
                  skip(db)

                  replacements += (lhs -> dbSym)
                  defs += (dbSym -> db)
                  db
                } else {
                  vd
                }

              // check if the user creates a DataBag using the .readText, .readCSV
              // we basically have to create custom functions for all DataBag-specific calls.
              case core.DefCall(_, DataBag$.readText, _, Seq(Seq(core.ValRef(argsym)))) =>
                val argSymRepl = replacements(argsym)
                val argReplRef = core.ValRef(argSymRepl)

                if (defcallargBecameSingSrc(argsym)) {
                  val dbRhs = core.DefCall(
                    Some(SingletonBagOps$.ref),
                    SingletonBagOps$.fromSingSrcReadText,
                    Seq(),
                    Seq(Seq(argReplRef))
                  )
                  val dbSym = newValSym(owner, "db", dbRhs)
                  val db = core.ValDef(dbSym, dbRhs)
                  skip(db)

                  replacements += (lhs -> dbSym)
                  defs += (dbSym -> db)
                  db
                } else {
                  vd
                }

              case core.DefCall(_, DataBag$.readCSV, targs, Seq(Seq(core.ValRef(pathSym), core.ValRef(formatSym)))) => {
                val pathSymRepl = replacements(pathSym)
                val pathSymReplRef = core.ValRef(pathSymRepl)
                val formatSymRepl = replacements(formatSym)
                val formatSymReplRef = core.ValRef(formatSymRepl)

                if (defcallargBecameSingSrc(pathSym) && defcallargBecameSingSrc(formatSym)) {
                  val dbRhs = core.DefCall(
                    Some(SingletonBagOps$.ref),
                    SingletonBagOps$.fromSingSrcReadCSV,
                    targs,
                    Seq(Seq(pathSymReplRef, formatSymReplRef))
                  )
                  val dbSym = newValSym(owner, "db", dbRhs)
                  val db = core.ValDef(dbSym, dbRhs)
                  skip(db)

                  replacements += (lhs -> dbSym)
                  defs += (dbSym -> db)
                  db
                } else {
                  vd
                }
              }

              case core.DefCall(Some(tgt), DataBag.writeCSV, _,
              Seq(Seq(core.ValRef(pathSym), core.ValRef(csvSym)))
              ) => {
                val pathSymRepl = replacements(pathSym)
                val csvSymRepl = replacements(csvSym)
                val pathReplRef = core.ValRef(pathSymRepl)
                val csvReplRef = core.ValRef(csvSymRepl)

                if (defcallargBecameSingSrc(pathSym) && defcallargBecameSingSrc(csvSym)) {
                  val dbRhs = core.DefCall(
                    Some(SingletonBagOps$.ref),
                    SingletonBagOps$.fromDataBagWriteCSV,
                    Seq(tgt.tpe.widen.typeArgs.head),
                    Seq(Seq(tgt, pathReplRef, csvReplRef))
                  )
                  val dbSym = newValSym(owner, "db", dbRhs)
                  val db = core.ValDef(dbSym, dbRhs)
                  skip(db)

                  replacements += (lhs -> dbSym)
                  defs += (dbSym -> db)
                  db}
                else {
                  vd
                }
              }

              // collect
              case dc @ core.DefCall(Some(core.ValRef(tgtSym)), DataBag.collect, _, _) =>
                val tgtSymRepl =
                  if (replacements.keys.toList.map(_.name).contains(tgtSym.name)) replacements(tgtSym)
                  else tgtSym
                val tgtRepl = core.ValRef(tgtSymRepl)

                val bagTpe = tgtSym.info.typeArgs.head
                val targsRepl = Seq(bagTpe)

                val ndc =core.DefCall(Some(SingletonBagOps$.ref), SingletonBagOps$.collect, targsRepl, Seq(Seq(tgtRepl)))
                val ndcRefDef = valRefAndDef(owner, "collect", ndc)

                val blockFinal = core.Let(Seq(ndcRefDef._2), Seq(), ndcRefDef._1)
                val blockFinalSym = newValSym(owner, "db", blockFinal)
                val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
                skip(blockFinalRefDef._2)

                replacements += (lhs -> blockFinalSym)
                defs += (blockFinalSym -> blockFinalRefDef._2)

                blockFinalRefDef._2

              // fold1
              // catch all cases of DataBag[A] => B, transform to DataBag[A] => DataBag[B]
              // val db = ...
              // val alg: Alg[A,B] = ...
              // val nondb = db.fold[B](alg)
              //=========================
              // val db = ...
              // val alg = ...
              // val db = DB.foldToBagAlg[A,B](db, alg)
              case dc @ core.DefCall(Some(core.ValRef(tgtSym)), DataBag.fold1, targs, Seq(Seq(alg))) =>
                val tgtSymRepl =
                  if (replacements.keys.toList.map(_.name).contains(tgtSym.name)) replacements(tgtSym)
                  else tgtSym
                val tgtRepl = core.ValRef(tgtSymRepl)

                val inTpe = tgtSym.info.typeArgs.head
                val outTpe = targs.head
                val targsRepl = Seq(inTpe, outTpe)

                val ndc = core.DefCall(Some(SingletonBagOps$.ref), SingletonBagOps$.fold1, targsRepl, Seq(Seq(tgtRepl, alg)))
                val ndcRefDef = valRefAndDef(owner, "fold1", ndc)

                val blockFinal = core.Let(Seq(ndcRefDef._2), Seq(), ndcRefDef._1)
                val blockFinalSym = newValSym(owner, "db", blockFinal)
                val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
                skip(blockFinalRefDef._2)

                replacements += (lhs -> blockFinalSym)
                defs += (blockFinalSym -> blockFinalRefDef._2)

                blockFinalRefDef._2

              // fold2
              // val db = ...
              // val zero: B = ...
              // val init: (A => B) = ...
              // val plus: (B => B) = ...
              // val nondb = db.fold[B](zero)(init, plus)
              //=========================
              // val db = ...
              // val zero: B = ...
              // val init: (A => B) = ...
              // val plus: (B => B) = ...
              // val db = DB.foldToBag[A,B](db, zero, init, plus)
              case core.DefCall(Some(core.ValRef(tgtSym)), DataBag.fold2, targs, Seq(Seq(zero), Seq(init, plus))) =>
                zero match {
                  case core.Lit(_) => ()
                  case _ => assert(false, "not supported type in fold2: zero has to be literal")
                }
                val tgtSymRepl =
                  if (replacements.keys.toList.map(_.name).contains(tgtSym.name)) replacements(tgtSym)
                  else tgtSym
                val tgtRepl = core.ValRef(tgtSymRepl)

                val inTpe = tgtSym.info.typeArgs.head
                val outTpe = targs.head
                val targsRepl = Seq(inTpe, outTpe)

                val ndc = core.DefCall(Some(SingletonBagOps$.ref), SingletonBagOps$.fold2, targsRepl, Seq(Seq(tgtRepl, zero, init, plus)))
                val ndcRefDef = valRefAndDef(owner, "fold2", ndc)

                val blockFinal = core.Let(Seq(ndcRefDef._2), Seq(), ndcRefDef._1)
                val blockFinalSym = newValSym(owner, "db", blockFinal)
                val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
                skip(blockFinalRefDef._2)

                replacements += (lhs -> blockFinalSym)
                defs += (blockFinalSym -> blockFinalRefDef._2)

                blockFinalRefDef._2

              // if there is 1 non-constant argument inside the defcall, call map on argument databag
              case dc @ core.DefCall(_, _, _, _) if countSeenRefs(dc, replacements)==1 && !isDataBag(dc) =>
                val refs = dc.collect {
                  case vr @ core.ValRef(_) => vr
                  case pr @ core.ParRef(_) => pr
                }
                // here we have to use ref names to compare different refs refering to the same valdef
                val nonC = refs.filter(e => replacements.keys.toList.map(_.name).contains(e.name))

                val x = nonC.head match {
                  case core.ValRef(sym) => core.ValRef(replacements(sym))
                  case core.ParRef(sym) => core.ParRef(replacements(sym))
                }

                val lbdaSym = api.ParSym(owner, api.TermName.fresh("t"), nonC.head.tpe.widen)
                val lbdaRef = core.ParRef(lbdaSym)
                //   lambda = t -> {
                //     t.f(c1, ..., ck)(impl ...)
                //   }

                val m = Map(nonC.head -> lbdaRef)

                val lmbdaRhsDC = api.TopDown.transform{
                  case v @ core.ValRef(_) => if (m.keys.toList.contains(v)) m(v) else v
                  case p @ core.ParRef(_) => if (m.keys.toList.contains(p)) m(p) else p
                }._tree(dc)
                val lmbdaRhsDCRefDef = valRefAndDef(owner, "lbdaRhs", lmbdaRhsDC)
                skip(lmbdaRhsDCRefDef._2)
                val lmbdaRhs = core.Let(Seq(lmbdaRhsDCRefDef._2), Seq(), lmbdaRhsDCRefDef._1)
                val lmbda = core.Lambda(
                  Seq(lbdaSym),
                  lmbdaRhs
                )
                val lambdaRefDef = valRefAndDef(owner, "lambda", lmbda)

                val mapDC = core.DefCall(Some(x), DataBag.map, Seq(dc.tpe), Seq(Seq(lambdaRefDef._1)))
                val mapDCRefDef = valRefAndDef(owner, "map", mapDC)
                skip(mapDCRefDef._2)

                val blockFinal = core.Let(Seq(lambdaRefDef._2, mapDCRefDef._2), Seq(), mapDCRefDef._1)
                val blockFinalSym = newValSym(owner, "db", blockFinal)
                val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
                skip(blockFinalRefDef._2)

                replacements += (lhs -> blockFinalSym)
                defs += (blockFinalSym -> blockFinalRefDef._2)

                blockFinalRefDef._2

              // if there are 2 non-constant arguments inside the defcall, cross and apply the defcall method to the tuple
              case dc @ core.DefCall(_, _, _, _) if countSeenRefs(dc, replacements)==2 && !isDataBag(dc)  =>
                val refs = dc.collect{
                  case vr @ core.ValRef(_) => vr
                  case pr @ core.ParRef(_) => pr
                }
                // here we have to use ref names to compare different refs refering to the same valdef
                val nonC = refs.filter(e => replacements.keys.toList.map(_.name).contains(e.name))
                val nonCReplRefs = nonC.map {
                  case core.ValRef(sym) => core.ValRef(replacements(sym))
                  case core.ParRef(sym) => core.ParRef(replacements(sym))
                }

                val targsRepls = nonC.map(_.tpe.widen)
                val crossDc = core.DefCall(Some(Ops.ref), Ops.cross, targsRepls, Seq(nonCReplRefs))
                skip(crossDc)

                val x = nonC(0)
                val y = nonC(1)

                val xyTpe = api.Type.kind2[Tuple2](x.tpe, y.tpe)
                val lbdaSym = api.ParSym(owner, api.TermName.fresh("t"), xyTpe)
                val lbdaRef = core.ParRef(lbdaSym)
                //   lambda = t -> {
                //     t1 = t._1
                //     t2 = t._2
                //     t1.f(c1, ..., cn, t2, cn+2, ..., ck)(impl ...)
                //   }

                //     t1 = t._1
                val t1 = core.DefCall(Some(lbdaRef), _1, Seq(), Seq())
                val t1RefDef = valRefAndDef(owner, "t1", t1)
                skip(t1RefDef._2)

                //     t2 = t._2
                val t2 = core.DefCall(Some(lbdaRef), _2, Seq(), Seq())
                val t2RefDef = valRefAndDef(owner, "t2", t2)
                skip(t2RefDef._2)

                val m = Map(x -> t1RefDef._1, y -> t2RefDef._1)

                val lmbdaRhsDC = api.TopDown.transform{
                  case v @ core.ValRef(_) => if (m.keys.toList.contains(v)) m(v) else v
                  case p @ core.ParRef(_) => if (m.keys.toList.contains(p)) m(p) else p
                }._tree(dc)
                val lmbdaRhsDCRefDef = valRefAndDef(owner, "lbdaRhs", lmbdaRhsDC)
                skip(lmbdaRhsDCRefDef._2)
                val lmbdaRhs = core.Let(Seq(t1RefDef._2, t2RefDef._2, lmbdaRhsDCRefDef._2), Seq(), lmbdaRhsDCRefDef._1)
                val lmbda = core.Lambda(
                  Seq(lbdaSym),
                  lmbdaRhs
                )
                val lambdaRefDef = valRefAndDef(owner, "lambda", lmbda)

                val crossSym = newValSym(owner, "cross", crossDc)
                val crossRefDef = valRefAndDef(crossSym, crossDc)
                skip(crossRefDef._2)

                val mapDC = core.DefCall(Some(crossRefDef._1), DataBag.map, Seq(dc.tpe), Seq(Seq(lambdaRefDef._1)))
                val mapDCRefDef = valRefAndDef(owner, "map", mapDC)
                skip(mapDCRefDef._2)

                val blockFinal = core.Let(Seq(crossRefDef._2, lambdaRefDef._2, mapDCRefDef._2), Seq(), mapDCRefDef._1)
                val blockFinalSym = newValSym(owner, "db", blockFinal)
                val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
                skip(blockFinalRefDef._2)

                replacements += (lhs -> blockFinalSym)
                defs += (blockFinalSym -> blockFinalRefDef._2)

                blockFinalRefDef._2

              // if there are 3 non-constant arguments inside the defcall, cross and apply the defcall method to the tuple
              case dc @ core.DefCall(_, _, _, _) if countSeenRefs(dc, replacements)==3 && !isDataBag(dc)  =>
                val refs = dc.collect{
                  case vr @ core.ValRef(_) => vr
                  case pr @ core.ParRef(_) => pr
                }
                val nonC = refs.filter(e => replacements.keys.toList.map(_.name).contains(e.name))
                val nonCReplRefs = nonC.map {
                  case core.ValRef(sym) => core.ValRef(replacements(sym))
                  case core.ParRef(sym) => core.ParRef(replacements(sym))
                }

                val targsRepls = nonC.map(_.tpe.widen)
                val crossDc = core.DefCall(Some(SingletonBagOps$.ref), SingletonBagOps$.cross3, targsRepls, Seq(nonCReplRefs))
                skip(crossDc)

                val x = nonC(0)
                val y = nonC(1)
                val z = nonC(2)

                val xyzTpe = api.Type.kind3[Tuple3](x.tpe, y.tpe, z.tpe)
                val lbdaSym = api.ParSym(owner, api.TermName.fresh("t"), xyzTpe)
                val lbdaRef = core.ParRef(lbdaSym)
                //   lambda = t -> {
                //     t1 = t._1
                //     t2 = t._2
                //     t3 = t._3
                //     t1.f(c1, ..., cn, t2, cn+2, ..., ck)(impl ...)
                //   }

                //     t1 = t._1
                val t1 = core.DefCall(Some(lbdaRef), _3_1, Seq(), Seq())
                val t1RefDef = valRefAndDef(owner, "t1", t1)
                skip(t1RefDef._2)

                //     t2 = t._2
                val t2 = core.DefCall(Some(lbdaRef), _3_2, Seq(), Seq())
                val t2RefDef = valRefAndDef(owner, "t2", t2)
                skip(t2RefDef._2)

                //     t2 = t._3
                val t3 = core.DefCall(Some(lbdaRef), _3_3, Seq(), Seq())
                val t3RefDef = valRefAndDef(owner, "t3", t3)
                skip(t3RefDef._2)

                val m = Map(x -> t1RefDef._1, y -> t2RefDef._1, z -> t3RefDef._1)

                val lmbdaRhsDC = api.TopDown.transform{
                  case v @ core.ValRef(_) => if (m.keys.toList.contains(v)) m(v) else v
                  case p @ core.ParRef(_) => if (m.keys.toList.contains(p)) m(p) else p
                }._tree(dc)
                val lmbdaRhsDCRefDef = valRefAndDef(owner, "lbdaRhs", lmbdaRhsDC)
                skip(lmbdaRhsDCRefDef._2)
                val lmbdaRhs = core.Let(
                  Seq(t1RefDef._2, t2RefDef._2, t3RefDef._2, lmbdaRhsDCRefDef._2),
                  Seq(),
                  lmbdaRhsDCRefDef._1
                )
                val lmbda = core.Lambda(
                  Seq(lbdaSym),
                  lmbdaRhs
                )
                val lambdaRefDef = valRefAndDef(owner, "lambda", lmbda)

                val crossSym = newValSym(owner, "cross3", crossDc)
                val crossRefDef = valRefAndDef(crossSym, crossDc)
                skip(crossRefDef._2)

                val mapDC = core.DefCall(Some(crossRefDef._1), DataBag.map, Seq(dc.tpe), Seq(Seq(lambdaRefDef._1)))
                val mapDCRefDef = valRefAndDef(owner, "map", mapDC)
                skip(mapDCRefDef._2)

                val blockFinal = core.Let(Seq(crossRefDef._2, lambdaRefDef._2, mapDCRefDef._2), Seq(), mapDCRefDef._1)
                val blockFinalSym = newValSym(owner, "db", blockFinal)
                val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
                skip(blockFinalRefDef._2)

                replacements += (lhs -> blockFinalSym)
                defs += (blockFinalSym -> blockFinalRefDef._2)

                blockFinalRefDef._2

              case _ => {
                vd
              }

            }

          // if valdef rhs is not of type DataBag, turn it into a databag
          case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _) if !meta(vd).all.all.contains(SkipTraversal)
            && !refsSeen(rhs, replacements) && !isDataBag(rhs) && !isFun(lhs) && !hasFunInOwnerChain(lhs)
            && !isAlg(rhs) =>

            // create lambda () => rhs
            val rhsSym = newValSym(owner, "tmp", rhs)
            val rhsRefDef = valRefAndDef(rhsSym, rhs)
            skip(rhsRefDef._2)
            val lRhs = core.Let(Seq(rhsRefDef._2), Seq(), rhsRefDef._1)
            val l = core.Lambda(Seq(), lRhs)
            val lSym = newValSym(owner, "fun", l)
            val lRefDef = valRefAndDef(lSym, l)
            skip(lRefDef._2)

            val dbRhsDC = core.DefCall(Some(SingletonBagOps$.ref), SingletonBagOps$.singSrc, Seq(rhs.tpe), Seq(Seq(lRefDef._1)))
            val dbRhsDCSym = newValSym(owner, "dbRhs", dbRhsDC)
            val dbRhsDCRefDef = valRefAndDef(dbRhsDCSym, dbRhsDC)
            skip(dbRhsDCRefDef._1)
            skip(dbRhsDCRefDef._2)
            val dbRhs = core.Let(Seq(lRefDef._2, dbRhsDCRefDef._2), Seq(), dbRhsDCRefDef._1)
            val dbSym = newValSym(owner, "db", dbRhsDC)
            val db = core.ValDef(dbSym, dbRhs)
            skip(db)

            // save mapping of refs -> defs
            val dbDefs = db.collect{ case dbvd @ core.ValDef(ld, _) => (ld, dbvd) }
            dbDefs.map(t => defs += (t._1 -> t._2))

            replacements += (lhs -> dbSym)
            defs += (dbSym -> db)
            //postPrint(db)
            db

          // if we encounter a ParDef whose type is not DataBag (e.g. Int), databagify (e.g. DataBag[Int])
          case Attr.inh(pd @ core.ParDef(ts, _), owner::_) if !(ts.info.typeConstructor =:= API.DataBag.tpe)
            && !meta(pd).all.all.contains(SkipTraversal)
            && !hasFunInOwnerChain(ts) =>
            val nts = api.ParSym(
              owner,
              api.TermName.fresh("arg"),
              api.Type.apply(API.DataBag.tpe.typeConstructor, Seq(ts.info))
            )
            val npd = core.ParDef(nts)
            skip(npd)
            replacements += (ts -> nts)
            npd

          case Attr.inh(vr @ core.Ref(sym), _) =>
            if (replacements.keys.toList.contains(sym)) {
              val nvr = vr match {
                case core.ParRef(`sym`) => core.ParRef(replacements(sym))
                case core.ValRef(`sym`) => core.ValRef(replacements(sym))
              }
              skip(nvr)
              nvr
            } else {
              vr
            }

          case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _) if isAlg(rhs) =>
            replacements += (lhs -> lhs)
            vd

          // if we encounter a letblock whose expr is a DefCall with literals as arguments, create valdefs for literals,
          // prepend to valdefs of the letblock and replace DefCall args with references
          case Attr.inh(lb @ core.Let(valDefs, defDefs, core.DefCall(tgt, ms, targs, args)), owner :: _)
            if argsContainLiterals(args) =>

            var tmpDefs = Seq[u.ValDef]()
            val argsRepl = args.map{
              sl => sl.map {
                case lt@core.Lit(_) => {
                  val defSym = newValSym(owner, "anfLaby", lt)
                  val ltRefDef = valRefAndDef(defSym, lt)
                  tmpDefs = tmpDefs ++ Seq(ltRefDef._2)
                  ltRefDef._1
                }
                case (t: u.Tree) => t
              }
            }

            val dc = core.DefCall(tgt, ms, targs, argsRepl)

            val letOut = core.Let(
              tmpDefs ++ valDefs,
              defDefs,
              dc
            )

            letOut

        }._tree(tree)

      // second traversal to correct block types
      // Background: scala does not change block types if expression type changes
      // (see internal/Trees.scala - Tree.copyAttrs)
      val secondRun = api.BottomUp.unsafe
        .withOwner
        .transformWith {
          case Attr.inh(lb @ core.Let(valdefs, defdefs, expr), _) if lb.tpe != expr.tpe =>
            val nlb = core.Let(valdefs, defdefs, expr)
            nlb
        }._tree(firstRun)

      val unnested = Core.unnest(secondRun)

      meta(unnested).update(tree match {
        case core.Let(_,_,core.Ref(sym)) if isDataBag(sym) => OrigReturnType(true)
        case _ => OrigReturnType(false)
      })

      // postPrint(unnested)
      unnested

    })

    def argsContainLiterals(args: Seq[Seq[u.Tree]]): Boolean = {
      var out = false
      args.foreach{
        sl => sl.foreach{
          case core.Lit(_) => out = true
          case (t: u.Tree) => t
        }
      }
      out
    }

    case class SkipTraversal()
    def skip(t: u.Tree): Unit = {
      meta(t).update(SkipTraversal)
    }

    case class OrigReturnType(isBag: Boolean) // We are not using this at the moment

    /** check if a symbol refers to a function */
    def isFun(sym: u.TermSymbol): Boolean = api.Sym.funs(sym.info.dealias.widen.typeSymbol)
    def isFun(sym: u.Symbol): Boolean = api.Sym.funs(sym.info.dealias.widen.typeSymbol)

    /** Checks whether the given symbol has a function symbol in its owner chain (including itself!). */
    def hasFunInOwnerChain(sym: u.Symbol): Boolean = {
      var s = sym
      while (s != enclosingOwner && s != api.Sym.none) {
        if (isFun(s)) return true
        s = s.owner
      }
      false
    }

    def isAlg(tree: u.Tree): Boolean = {
      val out = tree.tpe.widen.typeConstructor.baseClasses.contains(API.Alg.sym)
      out
    }

    /** check if a tree is of type DataBag */
    def isDataBag(tree: u.Tree): Boolean = {
      isDataBag(tree.tpe)
    }

    def isDataBag(sym: u.Symbol): Boolean = {
      isDataBag(sym.info)
    }

    def isDataBag(tpe: u.Type): Boolean = {
      tpe.widen.typeConstructor =:= API.DataBag.tpe
    }

    def newValSym(own: u.Symbol, name: String, rhs: u.Tree): u.TermSymbol = {
      api.ValSym(own, api.TermName.fresh(name), rhs.tpe.widen)
    }

    def newParSym(own: u.Symbol, name: String, rhs: u.Tree): u.TermSymbol = {
      api.ParSym(own, api.TermName.fresh(name), rhs.tpe.widen)
    }

    def valRefAndDef(own: u.Symbol, name: String, rhs: u.Tree): (u.Ident, u.ValDef) = {
      val sbl = api.ValSym(own, api.TermName.fresh(name), rhs.tpe.widen)
      (core.ValRef(sbl), core.ValDef(sbl, rhs))
    }

    def valRefAndDef(sbl: u.TermSymbol, rhs: u.Tree): (u.Ident, u.ValDef) = {
      (core.ValRef(sbl), core.ValDef(sbl, rhs))
    }

    def countSeenRefs(t: u.Tree, m: scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]) : Int = {
      val refs = t.collect {
        case vr @ core.ValRef(_) => vr.name
        case pr @ core.ParRef(_) => pr.name
      }
      refs.foldLeft(0)((a,b) => a + (if (m.keys.toList.map(_.name).contains(b)) 1 else 0))
    }

    def refsSeen(t: u.Tree, m: scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]) : Boolean = {
      val refNames = t.collect {
        case vr @ core.ValRef(_) => vr
        case pr @ core.ParRef(_) => pr
      }.map(_.name)
      val seenNames = m.keys.toSeq.map(_.name)
      refNames.foldLeft(false)((a,b) => a || seenNames.contains(b))
    }

    val Seq(_1, _2) = {
      val tuple2 = api.Type[(Nothing, Nothing)]
      for (i <- 1 to 2) yield tuple2.member(api.TermName(s"_$i")).asTerm
    }

    val Seq(_3_1, _3_2, _3_3) = {
      val tuple3 = api.Type[(Nothing, Nothing, Nothing)]
      for (i <- 1 to 3) yield tuple3.member(api.TermName(s"_$i")).asTerm
    }

    object SingletonBagOps$ extends ModuleAPI {

      lazy val sym = api.Sym[SingletonBagOps.type].asModule

      val collect = op("collect")
      val fold1 = op("fold1")
      val fold2 = op("fold2")
      val fromSingSrcApply = op("fromSingSrcApply")
      val fromSingSrcReadText = op("fromSingSrcReadText")
      val fromSingSrcReadCSV = op("fromSingSrcReadCSV")
      val fromDataBagWriteCSV = op("fromDataBagWriteCSV")
      val singSrc = op("singSrc")

      val cross3 = op("cross3")

      override def ops = Set()

    }
  }
}

/**
 * Many bag operations have some scalar inputs/outputs. The operations in this object are variations of those
 * operations, with scalars replaced by singleton bags.
 *
 * (It's better to have this here at the top level of the file rather than in the object WrapScalars,
 * because there we have lots of name clashes with the DataBag reflection class, Meta in the compiler, etc.)
 */
object SingletonBagOps {

  // Note: the parameter has to be a lambda because the argument can only be a Ref. So if the parameter would be
  // simply A, then it would have to refer to a ValDef, whose rhs would have to be made a singSrc too, where we would
  // have the same problem again...
  def singSrc[A: Meta](l: () => A): DataBag[A] = {
    DataBag(Seq(l()))
  }

  def fromSingSrcApply[A: Meta](db: DataBag[Seq[A]]): DataBag[A] = {
    DataBag(db.collect().head)
  }

  def fromSingSrcReadText(db: DataBag[String]): DataBag[String] = {
    DataBag.readText(db.collect().head)
  }

  def fromSingSrcReadCSV[A: Meta : CSVConverter](
                                                  path: DataBag[String], format: DataBag[CSV]
                                                ): DataBag[A] = {
    DataBag.readCSV[A](path.collect().head, format.collect().head)
  }

  def fromDataBagWriteCSV[A: Meta](
                                    db: DataBag[A],
                                    path: DataBag[String],
                                    format: DataBag[CSV])(
                                    implicit converter: CSVConverter[A]
                                  ) : DataBag[Unit] = {
    singSrc( () => db.writeCSV(path.collect().head, format.collect().head)(converter) )
  }

  // fold Alg
  def fold1[A: Meta, B: Meta](db: DataBag[A], alg: Alg[A,B]): DataBag[B] = {
    DataBag(Seq(db.fold[B](alg)))
  }

  // fold classic
  def fold2[A: Meta, B: Meta]
  (db: DataBag[A], zero: B, init: A => B, plus: (B,B) => B): DataBag[B] = {
    DataBag(Seq(db.fold(zero)(init, plus)))
  }

  // fold2 from zero singSrc
  def fold2FromSingSrc[A: Meta, B: Meta]
  ( db: DataBag[A], zero: DataBag[B], init: A => B, plus: (B,B) => B ): DataBag[B] = {
    DataBag(Seq(db.fold(zero.collect().head)(init, plus)))
  }

  def collect[A: Meta](db: DataBag[A]): DataBag[Seq[A]] = {
    DataBag(Seq(db.collect()))
  }

  def cross3[A: Meta, B: Meta, C: Meta](
                                         xs: DataBag[A], ys: DataBag[B], zs: DataBag[C]
                                       )(implicit env: LocalEnv): DataBag[(A, B, C)] = for {
    x <- xs
    y <- ys
    z <- zs
  } yield (x, y, z)

}