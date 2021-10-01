package org.emmalanguage
package examples.ml

import api._

import scala.util.Random

@emma.lib
object KMeans {

  def apply(k: Int, inputFile: String, iterations: Int = 20): Unit = {

    val points = DataBag.readText(inputFile)
      .map { line =>
        val fields = line.split(",")
        Point(fields(0).toDouble, fields(1).toDouble)
      }

    var centroids = createRandomCentroids(k)

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
          .fold(Point(0,0))(identity, (p1, p2) => Point(p1.x + p2.x, p1.y + p2.y))
        val cnt = ps.size.toDouble
        val avg = Point(sum.x / cnt, sum.y / cnt)
        TaggedPoint(avg.x, avg.y, cid)
      }

      // resurrect "lost" centroids (that have not been nearest to ANY point)
      centroids = centroids union createRandomCentroids(k - centroids.size.toInt)
    }

    centroids.map(_.forgetTag)
  }

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
