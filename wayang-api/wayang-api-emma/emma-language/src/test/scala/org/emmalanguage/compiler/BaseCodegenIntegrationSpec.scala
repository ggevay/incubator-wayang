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

import api._
import test.schema.Graphs._
import test.schema.Math._
import test.schema.Movies._
import test.util._

import org.scalatest._
import org.scalatest.prop.PropertyChecks
import org.scalactic.Equality

import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom
import scala.util.Random

import java.io.File
import java.nio.file.Paths

abstract class BaseCodegenIntegrationSpec extends FreeSpec
  with Matchers
  with PropertyChecks
  with BeforeAndAfter
  with DataBagEquality
  with RuntimeCompilerAware {

  import BaseCodegenIntegrationSpec._

  import compiler._

  // ---------------------------------------------------------------------------
  // abstract trait methods
  // ---------------------------------------------------------------------------

  /** A function providing a backend context instance which lives for the duration of `f`. */
  def withBackendContext[T](f: Env => T): T

  val inputDir = new File(tempPath("test/input"))
  val outputDir = new File(tempPath("test/output"))
  implicit val imdbMovieCSVConverter = CSVConverter[ImdbMovie]

  lazy val actPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      transformations(loadConfig(baseConfig)) :+ addContext: _*
    ).compose(_.tree)

  lazy val expPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      addContext
    ).compose(_.tree)

  def verify[T: Equality](e: u.Expr[T]): Unit = {
    val actTree = actPipeline(e)
    val expTree = expPipeline(e)
    val actRslt = withBackendContext(eval[Env => T](actTree))
    val expRslt = withBackendContext(eval[Env => T](expTree))
    actRslt shouldEqual expRslt
  }

  def cancelIfWayang(): Unit = {
    assume(this.getClass.getSimpleName != "WayangCodegenIntegrationSpec", "Ignored for Wayang")
  }

  def ignoreForWayang(test: =>Unit): Unit = {
    cancelIfWayang()
    test
  }

  def show(x: Any): String = x match {
    case _: DataBag[_] => x.asInstanceOf[DataBag[_]].collect().toString
    case _ => x.toString
  }

  before {
    // make sure that the base paths exist
    inputDir.mkdirs()
    outputDir.mkdirs()
  }

  after {
    deleteRecursive(outputDir)
    deleteRecursive(inputDir)
  }

  // --------------------------------------------------------------------------
  // Filter
  // --------------------------------------------------------------------------

  "Filter" - {
    "strings" in verify(u.reify {
      DataBag(jabberwocky) withFilter { _.length > 10 }
    })

    "tuples" in verify(u.reify {
      DataBag(jabberwocky map {(_,1)}) withFilter { _._1.length > 10 }
    })

    "case classes" in verify(u.reify {
      DataBag(imdb)
        .withFilter { _.year > 1980 }
        .withFilter { _.title.length > 10 }
    })
  }

  // --------------------------------------------------------------------------
  // Map
  // --------------------------------------------------------------------------

  "Map" - {
    "primitives" in verify(u.reify {
      val us = DataBag(1 to 3)
      val vs = DataBag(4 to 6)
      val ws = DataBag(7 to 9)
      for {
        x <- DataBag(2 to 20 by 2)
      } yield {
        if (us.exists(_ == x)) 9 * x
        else if (vs.exists(_ == x)) 5 * x
        else if (ws.exists(_ == x)) 1 * x
        else 0
      }
    })

    "tuples" in verify(u.reify {
      for { edge <- DataBag((1, 4, "A") :: (2, 5, "B") :: (3, 6, "C") :: Nil) }
        yield if (edge._1 < edge._2) edge._1 -> edge._2 else edge._2 -> edge._1
    })

    "case classes" in {
      verify(u.reify {
        for { edge <- DataBag(graph) } yield
          if (edge.label == "B") LabelledEdge(edge.dst, edge.src, "B")
          else edge.copy(label = "Y")
      })
    }
  }

  // --------------------------------------------------------------------------
  // FlatMap
  // --------------------------------------------------------------------------

  "FlatMap" - {
    "strings" in verify(u.reify {
      DataBag(jabberwocky) flatMap { line =>
        DataBag(line split "\\W+" filter { word =>
          word.length > 3 && word.length < 9
        })
      }
    })

    "with filter" in verify(u.reify {
      DataBag(jabberwocky) flatMap { line =>
        DataBag(line split "\\W+" filter {
          word => word.length > 3 && word.length < 9
        })
      } withFilter { _.length > 5 }
    })

    "comprehension with uncorrelated result" in verify(u.reify {
      for {
        line <- DataBag(jabberwocky)
        word <- DataBag(line split "\\W+" filter { word =>
          word.length > 3 && word.length < 9
        }) if word.length > 5
      } yield word
    })

    "comprehension with correlated result" in verify(u.reify {
      for {
        line <- DataBag(jabberwocky)
        word <- DataBag(line split "\\W+")
      } yield (line, word)
    })
  }

  // --------------------------------------------------------------------------
  // Distinct and Union
  // --------------------------------------------------------------------------

  "Distinct" - {
    "strings" in verify(u.reify {
      DataBag(jabberwocky flatMap { _ split "\\W+" }).distinct
    })

    "tuples" in verify(u.reify {
      DataBag(jabberwocky.flatMap { _ split "\\W+" } map {(_,1)}).distinct
    })
  }

  "Union" in {
    verify(u.reify {
      DataBag(jabberwockyEven) union DataBag(jabberwockyOdd)
    })
  }

  // --------------------------------------------------------------------------
  // Join & Cross
  // --------------------------------------------------------------------------

  "Join" - {
    "two-way on primitives" in verify(u.reify {
      for {
        x <- DataBag(1 to 50)
        y <- DataBag(1 to 100)
        if x == 2 * y
      } yield (x, 2 * y, 2)
    })

    "two-way on tuples" in verify(u.reify {
      for {
        x <- DataBag(zipWithIndex(5 to 15))
        y <- DataBag(zipWithIndex(1 to 20))
        if x._1 == y._1
      } yield (x, y)
    })

    // Q: how many cannes winners are there in the IMDB top 100?
    "two-way on case classes" in verify(u.reify {
      val cannesTop100 = for {
        movie <- DataBag(imdb)
        winner <- DataBag(cannes)
        if (movie.title, movie.year) == (winner.title, winner.year)
      } yield ("Cannes", movie.year, winner.title)

      val berlinTop100 = for {
        movie <- DataBag(imdb)
        winner <- DataBag(berlin)
        if (movie.title, movie.year) == (winner.title, winner.year)
      } yield ("Berlin", movie.year, winner.title)

      berlinTop100 union cannesTop100
    })

    "multi-way on primitives" in verify(u.reify {
      for {
        x <- DataBag(1 to 10)
        y <- DataBag(1 to 20)
        z <- DataBag(1 to 100)
        if x * x + y * y == z * z
      } yield (x, y, z)
    })

    "multi-way on case classes with local input" in {
      // Q: how many Cannes or Berlinale winners are there in the IMDB top 100?
      verify(u.reify {
        val cannesTop100 = for {
          movie <- DataBag(imdb)
          winner <- DataBag(cannes)
          if (winner.title, winner.year) == (movie.title, movie.year)
        } yield (movie.year, winner.title)

        val berlinTop100 = for {
          movie <- DataBag(imdb)
          winner <- DataBag(berlin)
          if (winner.title, winner.year) == (movie.title, movie.year)
        } yield (movie.year, winner.title)

        cannesTop100 union berlinTop100
      })
    }
  }

  "Cross" in verify(u.reify {
    for {
      x <- DataBag(3 to 100 by 3)
      y <- DataBag(5 to 100 by 5)
    } yield x * y
  })

  // --------------------------------------------------------------------------
  // Group (with materialization) and FoldGroup (aggregations)
  // --------------------------------------------------------------------------

  "Group" - {
    "materialization" in verify(u.reify {
      DataBag(Seq(1)) groupBy Predef.identity
    })

    "materialization with closure" in verify(u.reify {
      val semiFinal = 8
      val bag = DataBag(new Random shuffle 0.until(100).toList)
      val top = for (g <- bag groupBy { _ % semiFinal })
        yield g.values.collect().sorted.take(semiFinal / 2).sum

      top.max
    })
  }

  "FoldGroup" - {
    "of primitives" in verify(u.reify {
      for (g <- DataBag(1 to 100 map { _ -> 0 }) groupBy { _._1 })
        yield g.values.map { _._2 }.sum
    })

    "of case classes" in verify(u.reify {
      for (yearly <- DataBag(imdb) groupBy { _.year })
        yield yearly.values.size
    })

    "of case classes multiple times" in verify(u.reify {
      val movies = DataBag(imdb)

      for (decade <- movies groupBy { _.year / 10 }) yield {
        val values = decade.values
        val total = values.size
        val avgRating = values.map { _.rating.toInt * 10 }.sum / (total * 10.0)
        val minRating = values.map { _.rating }.min
        val maxRating = values.map { _.rating }.max

        (s"${decade.key * 10} - ${decade.key * 10 + 9}",
          total, avgRating, minRating, maxRating)
      }
    })

    "with a complex key" in verify(u.reify {
      val yearlyRatings = DataBag(imdb)
        .groupBy { movie => (movie.year / 10, movie.rating.toInt) }

      for (yr <- yearlyRatings) yield {
        val (year, rating) = yr.key
        (year, rating, yr.values.size)
      }
    })

    // Ignored because of https://issues.apache.org/jira/browse/WAYANG-43
    "with duplicate group names" in ignoreForWayang(verify(u.reify {
      val movies = DataBag(imdb)

      val leastPopular = for {
        Group(decade, dmovies) <- movies groupBy { _.year / 10 }
      } yield (decade, dmovies.size, dmovies.map { _.rating }.min)

      val mostPopular = for {
        Group(decade, dmovies) <- movies groupBy { _.year / 10 }
      } yield (decade, dmovies.size, dmovies.map { _.rating }.max)

      (leastPopular, mostPopular)
    }))

    "with multiple groups in the same comprehension" in verify(u.reify {
      for {
        can10 <- DataBag(cannes) groupBy { _.year / 10 }
        ber10 <- DataBag(berlin) groupBy { _.year / 10 }
        if can10.key == ber10.key
      } yield (can10.values.size, ber10.values.size)
    })
  }

  // --------------------------------------------------------------------------
  // Fold (global aggregations)
  // --------------------------------------------------------------------------

  "Fold" - {
    "of an empty DataBag (nonEmpty)" in verify(u.reify {
      //(DataBag[Int]().nonEmpty, DataBag(1 to 3).nonEmpty)
    })

    "of primitives (fold)" in verify(u.reify {
      DataBag(0 until 100).fold(0)(Predef.identity, _ + _)
    })

    "of primitives (sum)" in verify(u.reify {
      DataBag(1 to 200).sum
    })

    "of case classes (count)" in verify(u.reify {
      DataBag(imdb).size
    })
  }

  // --------------------------------------------------------------------------
  // Expression normalization
  // --------------------------------------------------------------------------

  "Normalization" - {
    "of filters with simple predicates" in verify(u.reify {
      for {
        x <- DataBag(1 to 1000)
        if !(x > 5 || (x < 2 && x == 0)) || (x > 5 || !(x < 2))
      } yield x
    })

    "of filters with simple predicates and multiple inputs" in verify(u.reify {
      for {
        x <- DataBag(1 to 1000)
        y <- DataBag(100 to 200)
        if x < y || x + y < 100 && x % 2 == 0 || y / 2 == 0
      } yield y + x
    })

    "of filters with UDF predicates" in verify(u.reify {
      for {
        x <- DataBag(1 to 1000)
        if !(p1(x) || (p2(x) && p3(x))) || (p1(x) || !p2(x))
      } yield x
    })

    "of names of case classes" in verify(u.reify {
      val movies = DataBag(imdb)
      val years = for (mov <- movies) yield ImdbYear(mov.year)
      years forall { case iy @ ImdbYear(yr) => iy == ImdbYear(yr) }
    })

    "of local functions" in verify(u.reify {
      val double = (x: Int) => 2 * x
      val add = (x: Int, y: Int) => x + y

      val times2 = for { x <- DataBag(1 to 100) } yield double(x)
      val increment5 = for { x <- DataBag(1 to 100) } yield add(x, 5)

      times2 union increment5
    })
  }

  // --------------------------------------------------------------------------
  // CSV IO
  // --------------------------------------------------------------------------

  "CSV" - {
    "read/write case classes" in verify(u.reify {
      val inputPath = materializeResource("/cinema/imdb.csv")
      val outputPath = Paths.get(s"${System.getProperty("java.io.tmpdir")}/emma/cinema/imdb_written.csv").toString
      // Read it, write it, and then read it again
      val imdb = DataBag.readCSV[ImdbMovie]("file://" + inputPath, CSV())
      imdb.writeCSV("file://" + outputPath, CSV())
      DataBag.readCSV[ImdbMovie]("file://" + outputPath, CSV()).collect().sortBy(_.title)
    })
  }

  // --------------------------------------------------------------------------
  // Miscellaneous
  // --------------------------------------------------------------------------

  "Miscellaneous" - {
    "Pattern matching in `yield`" in verify(u.reify {
      val range = DataBag(zipWithIndex(0 to 100))
      val squares = for (ij <- range) yield ij match { case (i, j) => i + j }
      squares.sum
    })

    "Map with partial function" in verify(u.reify {
      val range = DataBag(zipWithIndex(0 to 100))
      val squares = range map { case (i, j) => i + j }
      squares.sum
    })

    "Destructuring of a generator" in verify(u.reify {
      val range = DataBag(zipWithIndex(0 to 100))
      val squares = for { (x, y) <- range } yield x + y
      squares.sum
    })

    "Intermediate value definition" in verify(u.reify {
      val range = DataBag(zipWithIndex(0 to 100))
      val squares = for (xy <- range; sqr = xy._1 * xy._2) yield sqr
      squares.sum
    })

    //noinspection ScalaUnusedSymbol
    "Root package capture" in verify(u.reify {
      val eu = "eu"
      val com = "com"
      val java = "java"
      val org = "org"
      val scala = "scala"
      DataBag(0 to 100).sum
    })

    "Constant expressions" in verify(u.reify {
      val as = for { _ <- DataBag(1 to 100) } yield 1 // map
      val bs = DataBag(101 to 200) flatMap { _ => DataBag(2 to 4) } // flatMap
      val cs = for { _ <- DataBag(201 to 300) if 5 == 1 } yield 5 // filter
      val ds = DataBag(301 to 400) withFilter { _ => true } // filter
      as union bs union cs union ds
    })

    "Updated tmp sink (sieve of Eratosthenes)" in verify(u.reify {
      val N = 20
      val payload = "#" * 100

      val positive = {
        var primes = DataBag(3 to N map { (_, payload) })
        var p = 2

        while (p <= math.sqrt(N)) {
          primes = for { (n, payload) <- primes if n > p && n % p != 0 } yield (n, payload)
          p = primes.map { _._1 }.min
        }

        primes map { _._1 }
      }

      val negative = {
        var primes = DataBag(-N to 3 map { (_, payload) })
        var p = -2

        while (p >= -math.sqrt(N)) {
          primes = for { (n, payload) <- primes if n < p && n % p != 0 } yield (n, payload)
          p = primes.map { _._1 }.max
        }

        primes map { _._1 }
      }

      positive union negative
    })

    "val destructuring" in verify(u.reify {
      val resource = "file://" + materializeResource("/cinema/imdb.csv")
      val imdbTop100 = DataBag.readCSV[ImdbMovie](resource, CSV())
      val ratingsPerDecade = for {
        group <- imdbTop100.groupBy(mov => (mov.year / 10, mov.rating.round))
      } yield {
        val (year, rating) = group.key
        (year, rating, group.values.size)
      }

      for {
        r <- ratingsPerDecade
        m <- imdbTop100
        if r == (m.year, m.rating.round, 1L)
      } yield (r, m)
    })
  }




  "xxxxxxxxxxxx" - {


    "kmeans" in verify(u.reify {

      val points = DataBag.readText(inputFile)
        .map { line =>
          val fields = line.split(",")
          Point(fields(0).toDouble, fields(1).toDouble)
        }

      var centroids = createRandomCentroids(k, new Random())

      for (_ <- 1 to iterations) {
        // update solution: label each point with its nearest cluster
        val solution = for (p <- points) yield {
          val closestCentroid = centroids.min(distanceTo(p))
          TaggedPoint(p.x, p.y, closestCentroid.centroidId)
        }

        // update centroid positions as mean of associated points
        centroids = for {
          Group(cid, ps) <- solution.groupBy(_.centroidId)
        } yield {
          val sum = ps.map(_.forgetTag)
            .fold(Point(0,0))(p => p, (p1, p2) => Point(p1.x + p2.x, p1.y + p2.y))
          val cnt = ps.size.toDouble
          val avg = Point(sum.x / cnt, sum.y / cnt)
          TaggedPoint(avg.x, avg.y, cid)
        }

        // resurrect "lost" centroids (that have not been nearest to ANY point)
        centroids = centroids union createRandomCentroids(k - centroids.size.toInt, new Random())
      }

      centroids.map(_.forgetTag)
    })
  }




  // =================================================

  import org.emmalanguage.compiler.lang._
  import org.emmalanguage.api.backend._
  import org.emmalanguage.api.alg._
  import org.emmalanguage.api

//  {
//    val fun$r10 = (() => {
//      val tmp$r1 = BaseCodegenIntegrationSpec.inputFile;
//      tmp$r1
//    });
//    val db$r1 = SingletonBagOps.singSrc[String](fun$r10);
//    val db$r2 = SingletonBagOps.fromSingSrcReadText(db$r1);
//    val f$r1 = ((line: String) => {
//      val fields = line.split(",");
//      val anf$r4 = fields.apply(0);
//      val anf$r5 = Predef.augmentString(anf$r4);
//      val anf$r6 = anf$r5.toDouble;
//      val anf$r7 = fields.apply(1);
//      val anf$r8 = Predef.augmentString(anf$r7);
//      val anf$r9 = anf$r8.toDouble;
//      val anf$r10 = Point.apply(anf$r6, anf$r9);
//      anf$r10
//    });
//    val points = db$r2.map[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point](f$r1);
//    val fun$r11 = (() => {
//      val tmp$r2 = BaseCodegenIntegrationSpec.k;
//      tmp$r2
//    });
//    val db$r3 = SingletonBagOps.singSrc[Int](fun$r11);
//    val fun$r12 = (() => {
//      val tmp$r3 = new scala.util.Random();
//      tmp$r3
//    });
//    val db$r4 = SingletonBagOps.singSrc[scala.util.Random](fun$r12);
//    val anf$r14 = BaseCodegenIntegrationSpec.createRandomCentroids(db$r3, db$r4);
//    val fun$r13 = (() => {
//      val tmp$r4 = Predef.intWrapper(1);
//      tmp$r4
//    });
//    val db$r5 = SingletonBagOps.singSrc[scala.runtime.RichInt](fun$r13);
//    val fun$r14 = (() => {
//      val tmp$r5 = BaseCodegenIntegrationSpec.iterations;
//      tmp$r5
//    });
//    val db$r6 = SingletonBagOps.singSrc[Int](fun$r14);
//    val cross$r1 = WayangOps.cross[scala.runtime.RichInt, Int](db$r5, db$r6);
//    val lambda$r1 = ((t$r1: (scala.runtime.RichInt, Int)) => {
//      val t1$r1 = t$r1._1;
//      val t2$r1 = t$r1._2;
//      val lbdaRhs$r1 = t1$r1.to(t2$r1);
//      lbdaRhs$r1
//    });
//    val db$r7 = cross$r1.map[scala.collection.immutable.Range.Inclusive](lambda$r1);
//    val lambda$r2 = ((t$r2: scala.collection.immutable.Range.Inclusive) => {
//      val lbdaRhs$r2 = t$r2.toIterator;
//      lbdaRhs$r2
//    });
//    val db$r8 = db$r7.map[Iterator[Int]](lambda$r2);
//    val fun$r15 = (() => {
//      val tmp$r6 = null.asInstanceOf[Int];
//      tmp$r6
//    });
//    val db$r9 = SingletonBagOps.singSrc[Int](fun$r15);
//    @org.emmalanguage.compiler.ir.DSCFAnnotations.whileLoop def while$r2(arg$r1: org.emmalanguage.api.DataBag[Int], centroids$p$r1: org.emmalanguage.api.DataBag[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint]): org.emmalanguage.api.DataBag[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point] = {
//      val lambda$r3 = ((t$r3: Iterator[Int]) => {
//        val lbdaRhs$r3 = t$r3.hasNext;
//        lbdaRhs$r3
//      });
//      val db$r10 = db$r8.map[Boolean](lambda$r3);
//      @org.emmalanguage.compiler.ir.DSCFAnnotations.loopBody def body$r1(): org.emmalanguage.api.DataBag[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point] = {
//        val lambda$r4 = ((t$r4: Iterator[Int]) => {
//          val lbdaRhs$r4 = t$r4.next();
//          lbdaRhs$r4
//        });
//        val db$r11 = db$r8.map[Int](lambda$r4);
//        val fun$r16 = (() => {
//          val tmp$r7 = BaseCodegenIntegrationSpec.k;
//          tmp$r7
//        });
//        val db$r12 = SingletonBagOps.singSrc[Int](fun$r16);
//        val fun$r17 = (() => {
//          val tmp$r8 = new scala.util.Random();
//          tmp$r8
//        });
//        val db$r13 = SingletonBagOps.singSrc[scala.util.Random](fun$r17);
//        val fun$r3 = ((x$54: org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint) => {
//          val anf$r30 = x$54.centroidId;
//          anf$r30
//        });
//        val f$r5 = ((ctx$r1: org.apache.wayang.core.function.ExecutionContext) => {
//          val centroids$p$r2 = WayangNtv.bag[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint](centroids$p$r1)(ctx$r1);
//          val f$r2 = ((p: org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point) => {
//            val anf$r22: org.emmalanguage.compiler.BaseCodegenIntegrationSpec.distanceTo.type = BaseCodegenIntegrationSpec.distanceTo;
//            val anf$r25 = p.x;
//            val anf$r26 = p.y;
//            val anf$r23 = anf$r22.apply(p);
//            val alg$Min$r1 = Min.apply[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint](anf$r23);
//            val app$Min$r1 = centroids$p$r2.fold[Option[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint]](alg$Min$r1);
//            val closestCentroid = app$Min$r1.get;
//            val anf$r27 = closestCentroid.centroidId;
//            val anf$r28 = TaggedPoint.apply(anf$r25, anf$r26, anf$r27);
//            anf$r28
//          });
//          f$r2
//        });
//        val solution$r1 = WayangNtv.map[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point, org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint](f$r5)(points);
//        val solution = WayangNtv.broadcast[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint, org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint](solution$r1, centroids$p$r1);
//        val fun$r18 = (() => {
//          val tmp$r9 = Point.apply(0.0, 0.0);
//          tmp$r9
//        });
//        val db$r14 = SingletonBagOps.singSrc[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point](fun$r18);
//        val fun$r6 = ((p$r1: org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point) => {
//          p$r1
//        });
//        val fun$Map$r1 = ((x$55: org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint) => {
//          val anf$r37 = x$55.forgetTag;
//          anf$r37
//        });
//        val fun$r7 = ((p1: org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point, p2: org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point) => {
//          val anf$r40 = p1.x;
//          val anf$r41 = p2.x;
//          val anf$r42 = anf$r40.+(anf$r41);
//          val anf$r43 = p1.y;
//          val anf$r44 = p2.y;
//          val anf$r45 = anf$r43.+(anf$r44);
//          val anf$r46 = Point.apply(anf$r42, anf$r45);
//          anf$r46
//        });
//        val alg$fold$r1 = Fold.apply[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point, org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point](db$r14, fun$r6, fun$r7);
//        val alg$Map$r1 = Map.apply[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint, org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point, org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point](fun$Map$r1, alg$fold$r1);
//        val alg$Alg2$r1 = Alg2.apply[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint, Long, org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point](Size, alg$Map$r1);
//        val anf$r31 = WayangOps.foldGroup[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint, (Long, org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point), Int](solution, fun$r3, alg$Alg2$r1);
//        val f$r3 = ((check$ifrefutable$6: org.emmalanguage.api.Group[Int,(Long, org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point)]) => {
//          val cid$r1 = check$ifrefutable$6.key;
//          val app$Alg2$r1 = check$ifrefutable$6.values;
//          val anf$r48 = app$Alg2$r1._1;
//          val sum = app$Alg2$r1._2;
//          val anf$r50 = sum.x;
//          val anf$r52 = sum.y;
//          val cnt = anf$r48.toDouble;
//          val anf$r51 = anf$r50./(cnt);
//          val anf$r53 = anf$r52./(cnt);
//          val avg = Point.apply(anf$r51, anf$r53);
//          val anf$r55 = avg.x;
//          val anf$r56 = avg.y;
//          val anf$r57 = TaggedPoint.apply(anf$r55, anf$r56, cid$r1);
//          anf$r57
//        });
//        val anf$r58 = anf$r31.map[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint](f$r3);
//        val fun$r19 = (() => {
//          val tmp$r10 = anf$r58.fold[Long](Size);
//          tmp$r10
//        });
//        val db$r15 = SingletonBagOps.singSrc[Long](fun$r19);
//        val lambda$r5 = ((t$r5: Long) => {
//          val lbdaRhs$r5 = t$r5.toInt;
//          lbdaRhs$r5
//        });
//        val db$r16 = db$r15.map[Int](lambda$r5);
//        val cross$r2 = WayangOps.cross[Int, Int](db$r12, db$r16);
//        val lambda$r6 = ((t$r6: (Int, Int)) => {
//          val t1$r2 = t$r6._1;
//          val t2$r2 = t$r6._2;
//          val lbdaRhs$r6 = t1$r2.-(t2$r2);
//          lbdaRhs$r6
//        });
//        val db$r17 = cross$r2.map[Int](lambda$r6);
//        val anf$r64 = BaseCodegenIntegrationSpec.createRandomCentroids(db$r17, db$r13);
//        val anf$r65 = anf$r58.union(anf$r64);
//        while$r2(db$r11, anf$r65)
//      };
//      @org.emmalanguage.compiler.ir.DSCFAnnotations.suffix def suffix$r1(): org.emmalanguage.api.DataBag[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point] = {
//        val f$r4 = ((x$57: org.emmalanguage.compiler.BaseCodegenIntegrationSpec.TaggedPoint) => {
//          val anf$r66 = x$57.forgetTag;
//          anf$r66
//        });
//        val anf$r67 = centroids$p$r1.map[org.emmalanguage.compiler.BaseCodegenIntegrationSpec.Point](f$r4);
//        anf$r67
//      };
//      if (db$r10)
//        body$r1()
//      else
//        suffix$r1()
//    };
//    while$r2(db$r9, anf$r14)
//  }

  // =================================================





}

object BaseCodegenIntegrationSpec {
  lazy val jabberwocky = fromPath(materializeResource("/lyrics/Jabberwocky.txt"))

  lazy val imdb = DataBag.readCSV[ImdbMovie]("file://" + materializeResource("/cinema/imdb.csv"), CSV()).collect()

  lazy val cannes =
    DataBag.readCSV[FilmFestWinner]("file://" + materializeResource("/cinema/canneswinners.csv"), CSV()).collect()

  lazy val berlin =
    DataBag.readCSV[FilmFestWinner]("file://" + materializeResource("/cinema/berlinalewinners.csv"), CSV()).collect()

  val (jabberwockyEven, jabberwockyOdd) = jabberwocky
    .flatMap { _ split "\\W+" }
    .partition { _.length % 2 == 0 }

  // Workaround for https://issues.scala-lang.org/browse/SI-9933
  def zipWithIndex[A, Repr, That](coll: IterableLike[A, Repr])(
    implicit bf: CanBuildFrom[Repr, (A, Int), That]
  ): That = {
    val b = bf(coll.repr)
    var i = 0
    for (x <- coll) {
      b += ((x, i))
      i += 1
    }
    b.result()
  }





  val k = 4
  val inputFile = getTestFileUrl("kmeans_tmp_delete_later.input")
  val iterations = 10

  def getTestFileUrl(fileName: String) =
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString

  case class Point(x: Double, y: Double)

  case class TaggedPoint(x: Double, y: Double, centroidId: Int) {
    def forgetTag = Point(x, y)
  }

  val distance: (Point, Point) => Double = (p1, p2) => {
    val dx = p1.x - p2.x
    val dy = p1.y - p2.y
    math.sqrt(dx * dx + dy * dy)
  }

  // orders points based on their distance to `pos`
  val distanceTo: Point => Ordering[TaggedPoint] =
    (pos: Point) => Ordering.by((x: TaggedPoint) => distance(pos, x.forgetTag))

  def createRandomCentroids(n: Int, random: Random = new Random()): DataBag[TaggedPoint] =
    DataBag(for (_ <- 1 to n) yield TaggedPoint(random.nextGaussian(), random.nextGaussian(), random.nextInt()))



}
