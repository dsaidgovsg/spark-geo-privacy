/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf

import com.holdenkarau.spark.testing.{PerTestSparkContext, SparkContextProvider}
import org.scalatest.Suite

import org.apache.spark.SparkConf

trait TestSparkContext extends PerTestSparkContext with SparkContextProvider { self: Suite =>
  override val conf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("PmpfTestSpark")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.shuffle.partitions", "4")
  }
}
