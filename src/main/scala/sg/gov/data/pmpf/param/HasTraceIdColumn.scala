/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.param

trait HasTraceIdColumn {
  var traceIdColumn: String = "id"

  def setTraceIdColumn(newTraceIdColumn: String): this.type = {
    traceIdColumn = newTraceIdColumn
    this
  }
}
