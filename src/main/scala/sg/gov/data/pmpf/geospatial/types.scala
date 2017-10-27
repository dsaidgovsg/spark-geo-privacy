/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial

import sg.gov.data.pmpf.utils.GeodesicMath.sphericalToCartesian

// noinspection ScalaStyle
// scalastyle:off
object types {
  type Longitude = Double
  type Latitude = Double

  type GeoPoint = (Longitude, Latitude)

  type X = Double
  type Y = Double
  type Z = Double

  type Point3D = (X, Y, Z)
  def Point3D(geoPoint: GeoPoint): Point3D = sphericalToCartesian(geoPoint)

  type AnnPoint3D = Array[Float]
  def AnnPoint3D(point: Point3D): AnnPoint3D = Array(point._1, point._2, point._3).map(_.toFloat)
  def AnnPoint3D(point: GeoPoint): AnnPoint3D = AnnPoint3D(Point3D(point))
}
