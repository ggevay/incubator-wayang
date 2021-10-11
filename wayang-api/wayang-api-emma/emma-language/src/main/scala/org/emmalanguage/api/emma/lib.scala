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
package api.emma

import compiler.MacroCompiler

import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.collection.mutable.ListBuffer
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

@compileTimeOnly("Enable macro paradise to expand macro annotations.")
class lib extends StaticAnnotation {

  def macroTransform(annottees: Any*): Any = macro libMacro.inlineImpl

}

private class libMacro(val c: whitebox.Context) extends MacroCompiler {

  import c.universe._

  /** The implementation of the @emma.lib macro.
   *
   * The annottee is either an object or a class, which will be considered Emma library functions, and thus
   * inlined.
   * - For objects, this is simple, because we just need to inline the methods.
   * - For classes, we need to inline the fields as well. (This is called "scalar replacement" in the compiler
   * optimization literature.)
   *
   * @param annottees a list with the tree of an object or a class or a class and an object
   * @return the resulting tree with the replacement
   */
  def inlineImpl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    // https://docs.scala-lang.org/overviews/macros/annotations.html "many-to-many mapping"
    annottees.map(_.tree) match {
      case Seq(md @ ModuleDef(_,_,_)) =>
        c.Expr(transformModule(md))
      case Seq(cd @ ClassDef(_,_,_,_)) =>
        c.Expr(transformClassAndModule(cd, createEmptyCompanionFor(cd)))
      case Seq(cd @ ClassDef(_,_,_,_), md @ ModuleDef(_,_,_)) =>
        c.Expr(transformClassAndModule(cd, md))
      case tree: Tree =>
        c.error(tree.pos, "Unexpected annottee for `@emma.lib` annotation. " +
          "@emma.lib can be used only on either an object or a class. " +
          "(When using it on a class, its companion object is automatically considered part of the library.)")
        c.Expr(tree)
    }
  }

  lazy val transformModule: Tree => Tree = {
    case ModuleDef(mods, name, Template(parents, self, body)) =>
      val res = ModuleDef(mods, name, Template(parents, self, body flatMap transformDefDefOfModule))
      //c.warning(tree.pos, c.universe.showCode(res))
      res
  }

  /**
   * Transform a DefDef tree that is in a module.
   *
   * This consists of the following tasks if the passed argument is a `DefDef` node.
   *
   * 1. Remove annotations associated with the `DefDef` node.
   * 2. Attach the serialized-as-string source of the `DefDef` in a matching value member.
   * 3. Annotate the `DefDef` with the name of the value member from (2).
   *
   * For example
   *
   * {{{
   * def add(x: Int, y: Int): Int = x + y
   * }}}
   *
   * will become
   *
   * {{{
   * val add$Q$m1: String = emma.quote {
   *   def add(x: Int, y: Int) = x + y
   * }
   *
   * @emma.src("add$Q$m1")
   * def add(x: Int, y: Int): Int = x + y
   * }}}
   */
  lazy val transformDefDefOfModule: Tree => List[Tree] = {
    case DefDef(mods, name, tparams, vparamss, tpt, rhs)
      if name != api.TermName.init =>
      // clear all existing annotations from the DefDef
      val clrDefDef = DefDef(mods mapAnnotations (_ => List.empty[Tree]), name, tparams, vparamss, tpt, rhs)
      // create a fresh name for the associated `emma.quote { <defdef code> }` ValDef
      val nameQ = api.TermName.fresh(s"${name.encodedName}$$Q")
      // quote the method definition in a `val $nameQx = emma.quote { <defdef code> }
      val quoteDef = q"val $nameQ = ${API.emma.sym}.quote { $clrDefDef }"
      // annotate the DefDef with the name of its associated source
      val annDefDef = DefDef(mods mapAnnotations append(src(nameQ.toString)), name, tparams, vparamss, tpt, rhs)
      // emit `val $nameQx = emma.quote { <defdef code> }; @emma.src("$nameQx") def $name = ...`
      List(quoteDef, annDefDef)
    case tree =>
      List(tree)
  }

  val srcSym = rootMirror.staticClass("org.emmalanguage.api.emma.src")

  /** Construct the AST for `@emma.src("$v")` annotation. */
  def src(v: String): Tree =
    api.Inst(srcSym.toTypeConstructor, argss = Seq(Seq(api.Lit(v))))

  /** Show the code for the given Tree and wrap it as a string literal. */
  def codeLiteralFor(tree: Tree): Literal =
    api.Lit(c.universe.showCode(tree))

  /** Append an annotation `ann` to an `annotations` list. */
  def append(ann: Tree)(annotations: List[Tree]): List[Tree] =
    annotations :+ ann

  /** Ensure a condition applies or exit with the given error message. */
  def ensure(condition: Boolean, message: => String): Unit =
    if (!condition) c.error(c.enclosingPosition, message)

  // -----------------------

  /**
   * Creates quoted versions method definitions, and adds annotations with the names of the quoted method definitions.
   * @return Transformed body (the annotations added to DefDefs), and list of quote ValDefs
   */
  def transformBodyAndCollectQuoteDefs(body: List[Tree]): (List[Tree], List[Tree]) = {
    val quoteDefs = ListBuffer[Tree]()
    val transformedBody = body.map {
      case DefDef(mods, name, tparams, vparamss, tpt, rhs)
        if name != api.TermName.init =>
        // clear all existing annotations from the DefDef
        val clrDefDef = DefDef(mods mapAnnotations (_ => List.empty[Tree]), name, tparams, vparamss, tpt, rhs)
        // create a fresh name for the associated `emma.quote { <defdef code> }` ValDef
        val nameQ = api.TermName.fresh(s"${name.encodedName}$$Q")
        // quote the method definition in a `val $nameQx = emma.quote { <defdef code> }
        val quoteDef = q"val $nameQ = ${API.emma.sym}.quote { $clrDefDef }"
        // annotate the DefDef with the name of its associated source
        val annDefDef = DefDef(mods mapAnnotations append(src(nameQ.toString)), name, tparams, vparamss, tpt, rhs)
        // emit `val $nameQx = emma.quote { <defdef code> }; @emma.src("$nameQx") def $name = ...`
        quoteDefs += quoteDef
        annDefDef
      case tree =>
        tree
    }
    (transformedBody, quoteDefs.toList)
  }

  def transformClassAndModule(cd: ClassDef, md: ModuleDef): Tree = {
    ensure(cd.name.toString == md.name.toString, s"Class and supposed companion object name don't match: ${cd.name.toString} == ${md.name.toString}")

    val (transformedCd, cdQuoteDefs) = cd match {
      case ClassDef(mods, name, tparams, Template(parents, self, body)) =>
        val (transformedBody, quoteDefs) = transformBodyAndCollectQuoteDefs(body)
        (ClassDef(mods, name, tparams, Template(parents, self, transformedBody)), quoteDefs)
    }

    val (transformedMd, mdQuoteDefs) = md match {
      case ModuleDef(mods, name, Template(parents, self, body)) =>
        val (transformedBody, quoteDefs) = transformBodyAndCollectQuoteDefs(body)
        (ModuleDef(mods, name, Template(parents, self, transformedBody)), quoteDefs)
    }

    val transformedMdPlusQuotes = transformedMd match {
      case ModuleDef(mods, name, Template(parents, self, body)) =>
        ModuleDef(mods, name, Template(parents, self, body ++ cdQuoteDefs ++ mdQuoteDefs))
    }

    val res = q"""
        $transformedCd
        $transformedMdPlusQuotes"""
    println(res)
    res
  }

  def createEmptyCompanionFor(cd: ClassDef): ModuleDef = {
//    case ClassDef(mods, name, tparams, Template(parents, self, body)) =>
//      import compat._
//      // See the scaladoc of TemplateExtractor
//      val selfSym = internal.newTermSymbol(NoSymbol, termNames.WILDCARD, NoPosition, NoFlags)
//      internal.setInfo(selfSym, NoType)
//      val template = Template(parents, ValDef(selfSym, EmptyTree), List.empty)
//      internal.setOwner(selfSym, template.symbol)
//      ModuleDef(NoMods, TermName(name.toString), template)
      q"object ${TermName(cd.name.toString)} {}".asInstanceOf[ModuleDef]
  }
}

