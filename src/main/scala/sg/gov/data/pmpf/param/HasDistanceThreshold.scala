/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.param

trait HasDistanceThreshold {
  var distanceThreshold: Double = 100

  def setDistanceThreshold(newDistanceThreshold: Double): this.type = {
    distanceThreshold = newDistanceThreshold
    this
  }
}
