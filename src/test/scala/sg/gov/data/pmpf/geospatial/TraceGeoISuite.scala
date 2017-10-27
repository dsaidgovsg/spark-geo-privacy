/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial

import java.sql.Timestamp

import scala.io.Source

import org.scalactic.Tolerance
import sg.gov.data.pmpf.{SparkFunSuite, TestSparkContext}
import sg.gov.data.pmpf.geospatial.budgetmanager.{BudgetManager, FixedRateBudgetManager, FixedUtilityBudgetManager, NaiveBudgetManager}
import sg.gov.data.pmpf.utils.{GPSLog, TimeUtils}

import org.apache.spark.sql.functions.{col, collect_list, lit}
import org.apache.spark.sql.hive.test.TestHiveContext

class TraceGeoISuite extends SparkFunSuite
  with Tolerance
  with TestSparkContext {

  private val RandomSeed: Long = 31337

  private val dataSample = Seq(
    (1.0, 1.0, "2016-08-01 19:30:00.000", "Entity A"),
    (2.0, 2.0, "2016-08-01 19:31:00.000", "Entity A"),
    (3.0, 3.0, "2016-08-01 19:32:00.000", "Entity A"))
    .map(x => (x._1, x._2, java.sql.Timestamp.valueOf(x._3), x._4))

  private val singleDataSample = Seq(
    (12.0, 3.0, "2016-10-01 19:30:00.000", "Entity A"),
    (23.0, 2.0, "2016-10-01 19:31:00.000", "Entity B"),
    (34.0, 1.0, "2016-10-01 19:32:00.000", "Entity C"))
    .map(x => (x._1, x._2, java.sql.Timestamp.valueOf(x._3), x._4))

  private val dfColumns = Seq("latitude", "longitude", "timestamp", "token_id")

  test("default parameters") {
    val traceGeoI = new TraceGeoI()
    assert(traceGeoI.traceIdColumn === "id")
    assert(traceGeoI.dateTimeColumn === "datetime")
    assert(traceGeoI.latitudeColumn === "latitude")
    assert(traceGeoI.longitudeColumn === "longitude")
    assert(Option(traceGeoI.budgetManager).isEmpty)
  }

  test("set parameters") {
    val traceGeoI = new TraceGeoI()
      .setTraceIdColumn("id_column")
      .setDateTimeColumn("datetime_column")
      .setLatitudeColumn("latitude_column")
      .setLongitudeColumn("longitude_column")
      .setBudgetManager(new NaiveBudgetManager())

    assert(traceGeoI.traceIdColumn === "id_column")
    assert(traceGeoI.dateTimeColumn === "datetime_column")
    assert(traceGeoI.latitudeColumn === "latitude_column")
    assert(traceGeoI.longitudeColumn === "longitude_column")
    assert(traceGeoI.budgetManager.isInstanceOf[BudgetManager])
  }

  test("jitter naive") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val naiveBudgetManager = new NaiveBudgetManager()
    val traceGeoI = new TraceGeoI()
      .setTraceIdColumn("token_id")
      .setDateTimeColumn("timestamp")
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setBudgetManager(naiveBudgetManager)
      .setRandomSeed(RandomSeed)

    val df = dataSample.toDF(dfColumns: _*)
    val jitteredDF = traceGeoI.transform(df)

    assert(traceGeoI.budgetManager.isInstanceOf[NaiveBudgetManager])
    assert(jitteredDF.columns.length === 4)
    assert(jitteredDF.columns.sameElements(df.columns))
    assert(df.count === jitteredDF.count)

    val jitteredLng = jitteredDF.agg(collect_list("longitude")).head.getSeq[Double](0)
    Seq(1.0003967446914122,
      2.0006511443431587,
      2.997741253689711).zip(jitteredLng).foreach(i => assert(i._1 === i._2 +- 0.00000001))

    val jitteredLat = jitteredDF.agg(collect_list("latitude")).head.getSeq[Double](0)
    Seq(1.0003858417054081,
      2.000275067223699,
      2.9996072431561815).zip(jitteredLat).foreach(i => assert(i._1 === i._2 +- 0.00000001))
  }

  test("jitter fixed rate trace with only one point") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val fixedRateBudgetManager = new FixedRateBudgetManager()
    val traceGeoI = new TraceGeoI()
      .setTraceIdColumn("token_id")
      .setDateTimeColumn("timestamp")
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setBudgetManager(fixedRateBudgetManager)
      .setRandomSeed(RandomSeed)

    val df = singleDataSample.toDF(dfColumns: _*)
    val jitteredDF = traceGeoI.transform(df)

    assert(traceGeoI.budgetManager.isInstanceOf[FixedRateBudgetManager])
    assert(jitteredDF.columns.length === 4)
    assert(jitteredDF.columns.sameElements(df.columns))
    assert(df.count === jitteredDF.count)

    val expectedLng = Map(
      "Entity C" -> 0.9959015579258987,
      "Entity B" -> 2.0010649073630655,
      "Entity A" -> 3.000610893475818
    )
    val jitteredLng = jitteredDF.select("token_id", "longitude").collect.toList
    jitteredLng.foreach(i => assert(expectedLng(i.getString(0)) === i.getDouble(1) +- 0.00000001))

    val expectedLat = Map(
      "Entity C" -> 33.99940830882633,
      "Entity B" -> 23.000414342362916,
      "Entity A" -> 12.000581209523569
    )
    val jitteredLat = jitteredDF.select("token_id", "latitude").collect.toList
    jitteredLat.foreach(i => assert(expectedLat(i.getString(0)) === i.getDouble(1) +- 0.00000001))
  }

  test("jitter fixed rate") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val fixedRateBudgetManager = new FixedRateBudgetManager()
    fixedRateBudgetManager.setBudgetRate(0.0)

    val traceGeoI = new TraceGeoI()
      .setTraceIdColumn("token_id")
      .setDateTimeColumn("timestamp")
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setBudgetManager(fixedRateBudgetManager)
      .setRandomSeed(RandomSeed)

    val df = dataSample.toDF(dfColumns: _*)
    val jitteredDF = traceGeoI.transform(df)

    assert(traceGeoI.budgetManager.isInstanceOf[FixedRateBudgetManager])
    assert(jitteredDF.columns.length === 4)
    assert(jitteredDF.columns.sameElements(df.columns))
    assert(df.count === jitteredDF.count)

    val jitteredLng = jitteredDF.agg(collect_list("longitude")).head.getSeq[Double](0)
    Seq(1.0, 1.0, 1.0).zip(jitteredLng)foreach(i => assert(i._1 === i._2 +- 0.00000001))

    val jitteredLat = jitteredDF.agg(collect_list("latitude")).head.getSeq[Double](0)
    Seq(1.0, 1.0, 1.0).zip(jitteredLat).foreach(i => assert(i._1 === i._2 +- 0.00000001))
  }

  test("jitter fixed utility") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val fixedUtilityBudgetManager = new FixedUtilityBudgetManager()
    val traceGeoI = new TraceGeoI()
      .setTraceIdColumn("token_id")
      .setDateTimeColumn("timestamp")
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setBudgetManager(fixedUtilityBudgetManager)
      .setRandomSeed(RandomSeed)

    val df = dataSample.toDF(dfColumns: _*)
    val jitteredDF = traceGeoI.transform(df)

    assert(traceGeoI.budgetManager.isInstanceOf[FixedUtilityBudgetManager])
    assert(jitteredDF.columns.length === 4)
    assert(jitteredDF.columns.sameElements(df.columns))
    assert(df.count === jitteredDF.count)

    val jitteredLng = jitteredDF.agg(collect_list("longitude")).head.getSeq[Double](0)
    Seq(1.0001019982516606,
      2.0001674013125994,
      2.9994193034936862).zip(jitteredLng).foreach(i => assert(i._1 === i._2 +- 0.00000001))

    val jitteredLat = jitteredDF.agg(collect_list("latitude")).head.getSeq[Double](0)
    Seq(1.0000991952393445,
      2.0000707164802543,
      2.999899027411213).zip(jitteredLat).foreach(i => assert(i._1 === i._2 +- 0.00000001))
  }

  test("jitter fixed utility - test data full") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val fixedUtilityBudgetManager = new FixedUtilityBudgetManager()
    fixedUtilityBudgetManager.setUtility(50.0)
    val traceGeoI = new TraceGeoI()
      .setTraceIdColumn("trajectoryNum")
      .setDateTimeColumn("timestamp")
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setBudgetManager(fixedUtilityBudgetManager)
      .setRandomSeed(RandomSeed)

    val df = sc.textFile("src/test/data/full.csv")
      .map(_.split(","))
      .map(d => GPSLog(TimeUtils.fromISO(d(3)), d(1).toDouble, d(0).toDouble))
      .toDF
      .withColumn("trajectoryNum", lit("1"))
    val jitteredDF = traceGeoI.transform(df)

    assert(traceGeoI.budgetManager.isInstanceOf[FixedUtilityBudgetManager])
    assert(jitteredDF.columns.length === 4)
    assert(jitteredDF.columns.sameElements(df.columns))
    assert(df.count === jitteredDF.count)

    val expectedMap = Source.fromFile("src/test/data/expected/full_trace_fixed_utility_50m.csv")
      .getLines
      .map(_.split(","))
      .toArray
      .collect{case Array(lat, lng, _, t) => (TimeUtils.fromISO(t), (lng.toDouble, lat.toDouble))}
      .toMap

    val jitteredTS = jitteredDF.agg(collect_list("timestamp")).head.getSeq[Timestamp](0)
    expectedMap.keys.toSeq.sortBy(_.getTime).zip(jitteredTS).foreach(i => assert(i._1 === i._2))

    val jitteredLng = jitteredDF.sort("timestamp").select("timestamp", "longitude").collect.toList
    jitteredLng.foreach(i =>
      assert(expectedMap(i.getAs[Timestamp](0))._1 === i.getDouble(1) +- 0.00000001))

    val jitteredLat = jitteredDF.sort("timestamp").select("timestamp", "latitude").collect.toList
    jitteredLat.foreach(i =>
      assert(expectedMap(i.getAs[Timestamp](0))._2 === i.getDouble(1) +- 0.00000001))
  }

  test("jitter fixed utility - test data simplified") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val fixedUtilityBudgetManager = new FixedUtilityBudgetManager()
    fixedUtilityBudgetManager.setUtility(200.0)
    val traceGeoI = new TraceGeoI()
      .setTraceIdColumn("trajectoryNum")
      .setDateTimeColumn("timestamp")
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setBudgetManager(fixedUtilityBudgetManager)
      .setRandomSeed(RandomSeed)

    val df = sc.textFile("src/test/data/simplified.csv")
      .map(_.split(","))
      .map(d => GPSLog(TimeUtils.fromISO(d(3)), d(1).toDouble, d(0).toDouble))
      .toDF
      .withColumn("trajectoryNum", lit("1"))
    val jitteredDF = traceGeoI.transform(df).orderBy("timestamp")

    assert(traceGeoI.budgetManager.isInstanceOf[FixedUtilityBudgetManager])
    assert(jitteredDF.columns.length === 4)
    assert(jitteredDF.columns.sameElements(df.columns))
    assert(df.count === jitteredDF.count)

    val jitteredLng = jitteredDF.agg(collect_list("longitude")).head.getSeq[Double](0)
    Seq(103.77659992836885,
      103.77698768353486,
      103.77698768353486,
      103.77698768353486,
      103.77477897867973,
      103.77477897867973,
      103.77477897867973,
      103.77403286618562,
      103.7752653841741).zip(jitteredLng).foreach(i => assert(i._1 === i._2 +- 0.00000001))

    val jitteredLat = jitteredDF.agg(collect_list("latitude")).head.getSeq[Double](0)
    Seq(1.3548887641732716,
      1.3621240903543381,
      1.3621240903543381,
      1.3621240903543381,
      1.3601096000975912,
      1.3601096000975912,
      1.3601096000975912,
      1.3561013098722303,
      1.354921288422987).zip(jitteredLat).foreach(i => assert(i._1 === i._2 +- 0.00000001))
  }

  test("jitter naive - test data simplified") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val naiveBudgetManager = new NaiveBudgetManager()
    val traceGeoI = new TraceGeoI()
      .setTraceIdColumn("trajectoryNum")
      .setDateTimeColumn("timestamp")
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setBudgetManager(naiveBudgetManager)
      .setRandomSeed(RandomSeed)

    val df = sc.textFile("src/test/data/simplified.csv")
      .map(_.split(","))
      .map(d => GPSLog(TimeUtils.fromISO(d(3)), d(1).toDouble, d(0).toDouble))
      .toDF
      .withColumn("trajectoryNum", lit("1"))
    val jitteredDF = traceGeoI.transform(df).orderBy("timestamp")

    assert(traceGeoI.budgetManager.isInstanceOf[NaiveBudgetManager])
    assert(jitteredDF.columns.length === 4)
    assert(jitteredDF.columns.sameElements(df.columns))
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

  test("jitter fixed utility - with extra columns") {
    val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val fixedUtilityBudgetManager = new FixedUtilityBudgetManager()
    fixedUtilityBudgetManager.setUtility(200.0)
    val traceGeoI = new TraceGeoI()
      .setTraceIdColumn("trajectoryNum")
      .setDateTimeColumn("timestamp")
      .setLatitudeColumn("latitude")
      .setLongitudeColumn("longitude")
      .setBudgetManager(fixedUtilityBudgetManager)
      .setRandomSeed(RandomSeed)

    val df = sc.textFile("src/test/data/full.csv")
      .map(_.split(","))
      .map(d => GPSLog(TimeUtils.fromISO(d(3)), d(1).toDouble, d(0).toDouble))
      .toDF
      .withColumn("trajectoryNum", lit("1"))
      .withColumn("timestampExtra", col("timestamp"))
      .withColumn("trajectoryNumExtra", col("trajectoryNum"))
    val jitteredDF = traceGeoI.transform(df).orderBy("timestamp")

    assert(jitteredDF.columns.length === df.columns.length)
    assert(jitteredDF.columns.sameElements(df.columns))
    assert(df.count === jitteredDF.count)

    val jitteredTimeStamp =
      jitteredDF.agg(collect_list("timestamp")).head.getSeq[Timestamp](0)

    val jitteredTimeStampExtra =
      jitteredDF.agg(collect_list("timestampExtra")).head.getSeq[Timestamp](0)

    jitteredTimeStamp.zip(jitteredTimeStampExtra).foreach(x => assert(x._1 === x._2))

    val jitteredTrajectoryNumExtra =
      jitteredDF.agg(collect_list("trajectoryNumExtra")).head.getSeq[String](0)

    jitteredTrajectoryNumExtra.foreach(x => assert(x === "1"))
  }
}
