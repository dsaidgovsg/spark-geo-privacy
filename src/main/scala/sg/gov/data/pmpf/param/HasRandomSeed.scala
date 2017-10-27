/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.param

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{rand => sparkSQLRand}

trait HasRandomSeed {
  var randomSeed: Long = Long.MinValue

  def setRandomSeed(newRandomSeed: Long): this.type = {
    randomSeed = newRandomSeed
    this
  }

  def rand(): Column = randomSeed match {
    case Long.MinValue => sparkSQLRand()
    case i => sparkSQLRand(i)
  }
}
