/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.param

trait HasScale {
  var scale: Double = 100

  def setScale(newScale: Double): this.type = {
    scale = newScale
    this
  }
}
