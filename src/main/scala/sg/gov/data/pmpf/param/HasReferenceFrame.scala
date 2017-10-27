/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.param

import org.apache.spark.sql.DataFrame

trait HasReferenceFrame {
  var referenceFrame: DataFrame = _

  def setReferenceFrame(newReferenceFrame: DataFrame): this.type = {
    referenceFrame = newReferenceFrame
    this
  }
}
