/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.param

trait HasDateTimeColumn {
  var dateTimeColumn: String = "datetime"

  def setDateTimeColumn(newDateTimeColumn: String): this.type = {
    dateTimeColumn = newDateTimeColumn
    this
  }
}
