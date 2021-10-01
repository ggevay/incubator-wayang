package org.emmalanguage.examples.ml

import org.emmalanguage.test.util.{addToClasspath, deleteRecursive, tempPath}
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import KMeans.Point
import org.emmalanguage.api.emma
import org.emmalanguage.WayangAware

import java.io.File

trait KMeansIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter with WayangAware {

  private def getTestFileUrl(fileName: String) =
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString

  val codegenDir = tempPath("codegen")

  before {
    new File(codegenDir).mkdirs()
    addToClasspath(new File(codegenDir))
  }

  after {
    deleteRecursive(new File(codegenDir))
  }

  it should "run" in {
    val centroids = kmeans(k = 4,
      inputFile = getTestFileUrl("kmeans-k4-10000.input"),
      iterations = 100)

    assertResult(4)(centroids.size)
  }

  def kmeans(k: Int, inputFile: String, iterations: Int = 20): Iterable[Point] =
    withDefaultWayangEnv(implicit flink => emma.onWayang {
      // read a bag of directed edges
      // and convert it into an undirected bag without duplicates
      val incoming = DataBag.readCSV[Edge[Long]](input, csv)
      val outgoing = incoming.map(e => Edge(e.dst, e.src))
      val edges = (incoming union outgoing).distinct
      // compute all triangles
      val triangles = EnumerateTriangles(edges)
      // count and return the number of enumerated triangles
      triangles.size
    })
}
