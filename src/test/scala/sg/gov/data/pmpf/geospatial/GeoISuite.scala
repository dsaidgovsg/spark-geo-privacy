/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial

import org.scalactic.Tolerance
import sg.gov.data.pmpf.{SparkFunSuite, TestSparkContext}
import sg.gov.data.pmpf.utils.{GPSLog, TimeUtils}

import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.hive.test.TestHiveContext

class GeoISuite extends SparkFunSuite
  with Tolerance
  with TestSparkContext {

  private val RandomSeed: Long = 31337

  test("default parameters") {
    val geoi = new GeoI()

    assert(geoi.scale === 100)
    assert(geoi.latitudeColumn === "latitude")
    assert(geoi.longitudeColumn === "longitude")
  }

  test("set parameters") {
    val geoi = new GeoI()
      .setScale(200)
      .setLatitudeColumn("latitude_column")
      .setLongitudeColumn("longitude_column")

    assert(geoi.scale === 200)
    assert(geoi.latitudeColumn === "latitude_column")
    assert(geoi.longitudeColumn === "longitude_column")
  }

  test("jitter") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val geoi = new GeoI()
      .setScale(100)
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")

    val df = Seq(
      (1.0, 1.0, "2016-08-01 19:30:00.000", "Entity A"),
      (2.0, 2.0, "2016-08-01 19:31:00.000", "Entity A"),
      (3.0, 3.0, "2016-08-01 19:32:00.000", "Entity A"))
      .toDF("latitude", "longitude", "timestamp", "token")

    val jitteredDF = geoi.setRandomSeed(RandomSeed).transform(df).cache()

    assert(jitteredDF.columns.length === 4)
    assert(df.count === jitteredDF.count)

    val jitteredLng = jitteredDF.agg(collect_list("longitude")).head.getSeq[Double](0)
    Seq(1.0003967446914122, 2.0006511443431587, 2.997741253689711).zip(jitteredLng)
      .foreach(i => assert(i._1 === i._2 +- 0.00000001))

    val jitteredLat = jitteredDF.agg(collect_list("latitude")).head.getSeq[Double](0)
    Seq(1.0003858417054081, 2.000275067223699, 2.9996072431561815).zip(jitteredLat)
      .foreach(i => assert(i._1 === i._2 +- 0.00000001))
  }

  test("jitter - test data simplified") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val geoi = new GeoI()
      .setScale(100)
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setRandomSeed(RandomSeed)

    val df = sc.textFile("src/test/data/simplified.csv")
      .map(_.split(","))
      .map(d => GPSLog(TimeUtils.fromISO(d(3)), d(1).toDouble, d(0).toDouble))
      .toDF

    val jitteredDF = geoi.transform(df).cache()

    assert(jitteredDF.columns.length === 3)
    assert(df.count === jitteredDF.count)

    val expectedDF = sc.textFile("src/test/data/expected/simplified_naive_100m.csv")
      .map(_.split(","))
      .map(d => GPSLog(TimeUtils.fromISO(d(3)), d(1).toDouble, d(0).toDouble))
      .toDF

    val expectedLng = expectedDF.agg(collect_list("longitude")).head.getSeq[Double](0)
    val jitteredLng = jitteredDF.agg(collect_list("longitude")).head.getSeq[Double](0)
    expectedLng.zip(jitteredLng).foreach(i => assert(i._1 === i._2 +- 0.00000001))

    val expectedLat = expectedDF.agg(collect_list("latitude")).head.getSeq[Double](0)
    val jitteredLat = jitteredDF.agg(collect_list("latitude")).head.getSeq[Double](0)
    expectedLat.zip(jitteredLat).foreach(i => assert(i._1 === i._2 +- 0.00000001))
  }
}
