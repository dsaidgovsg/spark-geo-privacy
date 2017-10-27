/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.param

trait HasLongitudeColumn {
  var longitudeColumn: String = "longitude"

  def setLongitudeColumn(newLongitudeColumn: String): this.type = {
    longitudeColumn = newLongitudeColumn
    this
  }
}
