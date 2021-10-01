package org.emmalanguage.examples.ml

import org.emmalanguage.test.util.{addToClasspath, deleteRecursive, tempPath}
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import KMeans.Point
import org.emmalanguage.api.emma
import org.emmalanguage.WayangAware

import java.io.File

class KMeansIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter with WayangAware {

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
      inputFile = getTestFileUrl("kmeans.input"),
      iterations = 100)

    assertResult(4)(centroids.size)
  }

  def kmeans(k: Int, inputFile: String, iterations: Int = 20): Iterable[Point] =
    withDefaultWayangEnv(implicit wayang => emma.onWayang {
      KMeans(k, inputFile, iterations).collect()
    })
}
