/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.utils

import org.scalactic.Tolerance
import sg.gov.data.pmpf.SparkFunSuite

class GeodesicMathSuite extends SparkFunSuite with Tolerance {

  test("get destination longitude and latitude") {
    val origin = (103.8198, 1.3521)
    val (distance, bearing) = (10000, 10)
    val (destLon, destLat) = GeodesicMath.getDestination(origin, distance, bearing)
    assert(destLon === 103.8354 +- 0.0001)
    assert(destLat === 1.4406 +- 0.0001)
  }

  test("get distance") {
    val origin = (103.8198, 1.3521)
    val destination = (103.8354, 1.4411)
    val distance = GeodesicMath.getDistance(origin, destination)
    assert(distance === 10058.39 +- 0.01)
  }

  test("get bearing") {
    val origin = (103.8198, 1.3521)
    val destination = (103.8354, 1.4411)
    val bearing = GeodesicMath.getBearing(origin, destination)
    assert(bearing === 9.9388 +- 0.0001)
  }

  test("get mean coordinate - longitude wraparound") {
    val points = Seq((-170.0, 0.0), (170.0, 0.0))
    val coordMean = GeodesicMath.getMeanCoordinate(points)
    assert((coordMean._1 + 180.0) % 180.0 === 0.0 +- 0.00000001)
    assert(coordMean._2 === 0.0 +- 0.00000001)
  }

  test("get mean coordinate - poles") {
    val points = Seq((0.0, 80.0), (-0.0, 100.0))
    val meanCoord = GeodesicMath.getMeanCoordinate(points)
    assert(meanCoord._1 === 0.0 +- 0.00000001)
    assert(meanCoord._2 === 90.0 +- 0.00000001)
  }

  test("get mean coordinate - zero") {
    val points = Seq((0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0))
    val meanCoord = GeodesicMath.getMeanCoordinate(points)
    assert(meanCoord._1 === 0.0 +- 0.00000001)
    assert(meanCoord._2 === 0.0 +- 0.00000001)
  }

  test("get mean coordinate - singapore") {
    val points = Seq((103.7764392, 1.354688958),
      (103.7763575, 1.354682),
      (103.7763775, 1.354662789),
      (103.7763853, 1.354677838),
      (103.776419, 1.354735642),
      (103.7765168, 1.354752909))
    val meanCoord = GeodesicMath.getMeanCoordinate(points)
    assert(meanCoord._1 === 103.77641588 +- 0.00000001)
    assert(meanCoord._2 === 1.35470002 +- 0.00000001)
  }

  test("from lng/lat to cartesian coordinate") {
    val (lon, lat) = (103.7764392, 1.354688958)
    val (x, y, z) = (-0.23806753509570533, 0.9709608253880659, 0.023641579794523783)
    val result = GeodesicMath.sphericalToCartesian(lon, lat)
    assert(result._1 === x +- 0.0000000001)
    assert(result._2 === y +- 0.0000000001)
    assert(result._3 === z +- 0.0000000001)
  }

  test("from cartesian coordinate to lng/lat") {
    val (lon, lat) = (103.7764392, 1.354688958)
    val (x, y, z) = (-0.23806753509570533, 0.9709608253880659, 0.023641579794523783)
    val result = GeodesicMath.cartesianToSpherical(x, y, z)
    assert(result._1 === lon +- 0.0000000001)
    assert(result._2 === lat +- 0.0000000001)
  }
}
