/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf

// for use with SparkSession API in the distant future

// scalastyle:off space.after.comment.start
//import org.scalatest.{BeforeAndAfterAll, Suite}
//import org.apache.spark.sql.SparkSession

//trait TestSparkSession extends BeforeAndAfterAll { self: Suite =>
//  @transient var spark: SparkSession = _
//
//  override def beforeAll() {
//    super.beforeAll()
//
//    spark = SparkSession.builder()
//      .appName("PmpfTestSpark")
//      .master("local")
//      .config("spark.sql.shuffle.partitions", "4")
//      .getOrCreate()
//  }
//
//  override def afterAll() {
//    if (spark != null) {
//      spark.stop()
//    }
//    spark = null
//    super.afterAll()
//  }
//}
