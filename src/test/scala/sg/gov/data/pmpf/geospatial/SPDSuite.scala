/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial

import org.scalactic.Tolerance
import sg.gov.data.pmpf.{SparkFunSuite, TestSparkContext}
import sg.gov.data.pmpf.utils.{GPSLog, TimeUtils}

import org.apache.spark.sql.functions.{col, collect_list, lit}
import org.apache.spark.sql.hive.test.TestHiveContext

class SPDSuite extends SparkFunSuite with Tolerance
  with TestSparkContext {

  test("default parameters") {
    val spd = new SPD()

    assert(spd.traceIdColumn === "id")
    assert(spd.latitudeColumn === "latitude")
    assert(spd.longitudeColumn === "longitude")
    assert(spd.dateTimeColumn === "datetime")
    assert(spd.distanceThreshold === 200.0)
    assert(spd.timeThreshold === 1800.0)
  }

  test("set parameters") {
    val spd = new SPD()
      .setTraceIdColumn("traceID")
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setDateTimeColumn("datetime")
      .setDistanceThreshold(1123.0)
      .setTimeThreshold(60.66)

    assert(spd.traceIdColumn === "traceID")
    assert(spd.latitudeColumn === "latitude")
    assert(spd.longitudeColumn === "longitude")
    assert(spd.dateTimeColumn === "datetime")
    assert(spd.distanceThreshold === 1123.0)
    assert(spd.timeThreshold === 60.66)
  }

  test("spd test data full") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val df = sc.textFile("src/test/data/full.csv")
      .map(_.split(","))
      .map(d => GPSLog(TimeUtils.fromISO(d(3)), d(1).toDouble, d(0).toDouble))
      .toDF
      .withColumn("trajectoryNum", lit("1"))

    val spd = new SPD()
      .setTraceIdColumn("trajectoryNum")
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setDateTimeColumn("timestamp")
      .setDistanceThreshold(10.0)
      .setTimeThreshold(60.0)

    val dfSPD = spd.extract(df)

    // Assert shape of dataframe
    assert(dfSPD.columns.length === 5)
    assert(dfSPD.count === 9)

    // Assert correct dwell times in pois
    val dwellTimes = dfSPD
      .agg(collect_list(col("departTime").cast("long") - col("arriveTime").cast("long")))
      .head
      .getSeq[Long](0)

    Seq(101, 61, 95, 61, 71, 196, 74, 124, 128).zip(dwellTimes).foreach(i => assert(i._1 === i._2))

    // Assert longitudes of pois are correct
    val longitudes = dfSPD.agg(collect_list("longitude")).head.getSeq[Double](0)
    Seq(103.77639590594059,
      103.7766529903226,
      103.77657618804345,
      103.77657839838712,
      103.77447217638888,
      103.77446820000011,
      103.77447084266664,
      103.77378557539683,
      103.77656144806205).zip(longitudes).foreach(i => assert(i._1 === i._2 +- 0.0000000001))

    // Assert latitudes of pois are correct
    val latitudes = dfSPD.agg(collect_list("latitude")).head.getSeq[Double](0)
    Seq(1.3546903737326732,
      1.3619826573709677,
      1.3620397595760865,
      1.3620342072903222,
      1.359948966166667,
      1.359885169015152,
      1.3598670813866665,
      1.3559130202063499,
      1.354705217224806).zip(latitudes).foreach(i => assert(i._1 === i._2 +- 0.0000000001))
  }

  test("spd no pois found") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val df = Seq(
      (1.0, 1.0, TimeUtils.fromISO("2016-08-01T19:30:00Z"), "Entity A"),
      (2.0, 2.0, TimeUtils.fromISO("2016-08-01T19:31:00Z"), "Entity A"),
      (3.0, 3.0, TimeUtils.fromISO("2016-08-01T19:32:00Z"), "Entity A"))
      .toDF("lat", "lon", "timestamp", "token")

    val spd = new SPD()
      .setTraceIdColumn("token")
      .setLatitudeColumn("lat")
      .setLongitudeColumn("lon")
      .setDateTimeColumn("timestamp")

    val dfSPD = spd.extract(df)

    // Assert shape of dataframe
    assert(dfSPD.columns.length === 5)
    assert(dfSPD.count === 0)
  }

  test("schema of poi dataframe") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val df = Seq(
      (1.0, 1.0, TimeUtils.fromISO("2016-08-01T19:30:00Z"), "Entity A"),
      (2.0, 2.0, TimeUtils.fromISO("2016-08-01T19:31:00Z"), "Entity A"),
      (3.0, 3.0, TimeUtils.fromISO("2016-08-01T19:32:00Z"), "Entity A"))
      .toDF("lat", "lon", "timestamp", "token")

    val spd = new SPD()
      .setTraceIdColumn("token")
      .setLatitudeColumn("lat")
      .setLongitudeColumn("lon")
      .setDateTimeColumn("timestamp")

    val dfSPD = spd.extract(df)

    assert(dfSPD.columns.toSet === Set("token", "lat", "lon", "arriveTime", "departTime"))
    assert(dfSPD.schema("lat").dataType.simpleString === "double")
    assert(dfSPD.schema("lon").dataType.simpleString === "double")
    assert(dfSPD.schema("arriveTime").dataType.simpleString === "timestamp")
    assert(dfSPD.schema("departTime").dataType.simpleString === "timestamp")
  }
}
