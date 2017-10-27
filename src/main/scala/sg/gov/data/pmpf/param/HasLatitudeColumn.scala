/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.param

trait HasLatitudeColumn {
  var latitudeColumn: String = "latitude"

  def setLatitudeColumn(newLatitudeColumn: String): this.type = {
    latitudeColumn = newLatitudeColumn
    this
  }
}
