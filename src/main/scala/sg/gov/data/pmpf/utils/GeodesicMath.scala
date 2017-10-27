/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.utils

import scala.math.{atan2, cos, sin, sqrt}

import org.locationtech.spatial4j.context.SpatialContext.GEO
import org.locationtech.spatial4j.distance.DistanceUtils._
import sg.gov.data.pmpf.geospatial.types._

/*
 * Geodesic calculator with spherical Earth geometry
 */
object GeodesicMath {
  val EARTH_RADIUS: Double = EARTH_EQUATORIAL_RADIUS_KM * 1000  // 6378137.0

  def getDestination(origin: GeoPoint, distance: Double, bearing: Double): GeoPoint = {
    val geoOrigin = GEO.getShapeFactory.pointXY(origin._1, origin._2)
    val distDEG = dist2Degrees(distance, EARTH_RADIUS)
    val destination = GEO.getDistCalc.pointOnBearing(geoOrigin, distDEG, bearing, GEO, geoOrigin)
    (destination.getX, destination.getY)
  }

  def getDistance(origin: GeoPoint, destination: GeoPoint): Double = {
    val geoOrigin = GEO.getShapeFactory.pointXY(origin._1, origin._2)
    val geoDestination = GEO.getShapeFactory.pointXY(destination._1, destination._2)
    val distDEG = GEO.getDistCalc.distance(geoOrigin, geoDestination)
    degrees2Dist(distDEG, EARTH_RADIUS)
  }

  def getBearing(origin: GeoPoint, destination: GeoPoint): Double = {
    val lonDiffRAD = toRadians(destination._1 - origin._1)
    val Seq(originLatRAD, destLatRAD) = Seq(origin._2, destination._2) map toRadians
    val y = sin(lonDiffRAD) * cos(destLatRAD)
    val x = cos(originLatRAD) * sin(destLatRAD) -
      sin(originLatRAD) * cos(destLatRAD) * cos(lonDiffRAD)
    toDegrees(atan2(y, x))
  }

  /*
   * Geographic mean of cartesian coordinates as defined in `LatLon.meanOf` in
   * http://www.movable-type.co.uk/scripts/latlong-vectors.html
   */
  def getMeanCoordinate(gpsSphericalPoints: Seq[GeoPoint]): GeoPoint = {
    val gpsCartesianPoints = gpsSphericalPoints.map(i => sphericalToCartesian(i._1, i._2))
    val sumX = gpsCartesianPoints.map(_._1).sum
    val sumY = gpsCartesianPoints.map(_._2).sum
    val sumZ = gpsCartesianPoints.map(_._3).sum
    val length = sqrt(sumX * sumX + sumY * sumY + sumZ * sumZ)

    if (length == 0 || length == 1) {
      /* if the vector is already unit or is zero magnitude, this is a no-op. */
      cartesianToSpherical(sumX, sumY, sumZ)
    } else {
      cartesianToSpherical(sumX / length, sumY / length, sumZ / length)
    }
  }

  /*
   * Converts lon/lat point to cartesian vector as defined in `LatLon.prototype.toVector` in
   * http://www.movable-type.co.uk/scripts/latlong-vectors.html
   */
  def sphericalToCartesian(geoPoint: GeoPoint): Point3D = {
    val lonRAD = toRadians(geoPoint._1)
    val latRAD = toRadians(geoPoint._2)
    val x = cos(latRAD) * cos(lonRAD)
    val y = cos(latRAD) * sin(lonRAD)
    val z = sin(latRAD)
    (x, y, z)
  }

  /*
   * Converts cartesian vector to lon/lat as define in `Vector3d.prototype.toLatLonS` in
   * http://www.movable-type.co.uk/scripts/latlong-vectors.html
   */
  def cartesianToSpherical(point: Point3D): GeoPoint = {
    val (x, y, z) = point
    val latRAD = atan2(z, sqrt(x * x + y * y))
    val lonRAD = atan2(y, x)
    (toDegrees(lonRAD), toDegrees(latRAD))
  }
}
