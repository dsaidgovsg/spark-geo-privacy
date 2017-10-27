/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial

import sg.gov.data.pmpf.{SparkFunSuite, TestSparkContext}

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{lit, mean}
import org.apache.spark.sql.types._

class PointRecallSuite extends SparkFunSuite
  with TestSparkContext {
  test("default parameters") {
    val pointRecall = new PointRecall()

    assert(pointRecall.traceIdColumn === "id")
    assert(pointRecall.longitudeColumn === "longitude")
    assert(pointRecall.latitudeColumn === "latitude")
    assert(pointRecall.distanceThreshold === 100)
  }

  test("set parameters") {
    val pointRecall = new PointRecall()
      .setTraceIdColumn("id_column")
      .setLongitudeColumn("longitude_column")
      .setLatitudeColumn("latitude_column")
      .setDistanceThreshold(10)

    assert(pointRecall.traceIdColumn === "id_column")
    assert(pointRecall.longitudeColumn === "longitude_column")
    assert(pointRecall.latitudeColumn === "latitude_column")
    assert(pointRecall.distanceThreshold === 10)
  }

  test("recall") {
    val sqlContext = new SQLContext(this.sc)
    import sqlContext.implicits._

    val originalPoints = Seq(
      ("1", "103.000", "1.350"),
      ("1", "103.001", "1.351"),
      ("1", "103.002", "1.352"),
      ("1", "103.003", "1.353"),
      ("2", "103.000", "1.350"),
      ("2", "102.009", "1.349"),
      ("2", "102.008", "1.348"),
      ("2", "102.007", "1.347"))
      .map(data => (data._1, data._2.toDouble, data._3.toDouble))
      .toDF("trajectoryId", "longitude", "latitude")

    val newPoints = Seq(
      ("1", "103.0001", "1.3501"),
      ("1", "103.0001", "1.3501"),
      ("1", "103.0001", "1.3501"),
      ("1", "103.0031", "1.3531"),
      ("2", "103.0001", "1.3501"),
      ("2", "102.0091", "1.3491"),
      ("2", "102.0081", "1.3481"),
      ("2", "102.0081", "1.3481"))
      .map(data => (data._1, data._2.toDouble, data._3.toDouble))
      .toDF("trajectoryId", "longitude", "latitude")

    val pointRecall = new PointRecall()
      .setReferenceFrame(originalPoints)
      .setTraceIdColumn("trajectoryId")
      .setLongitudeColumn("longitude")
      .setLatitudeColumn("latitude")
      .setDistanceThreshold(1000)

    /** apply the recall test to compare points of original with perturbed data */
    val results = pointRecall.compare(newPoints)

    val expectedColumns =
      List("trajectoryId", "_reference_points", "_comparison_points", "_recall")
    assert(results.select("trajectoryId").count === 2)
    assert(results.columns.forall(expectedColumns.contains))

    assert(results.where($"trajectoryId" === "1").first.getDouble(3) == 0.5)
    assert(results.where($"trajectoryId" === "2").first.getDouble(3) == 0.75)
  }

  test("no recall - both dataframes empty") {
    val sqlContext = new SQLContext(this.sc)

    val struct =
      StructType(Array(
        StructField("trajectoryId", IntegerType, true),
        StructField("longitude", DoubleType, false),
        StructField("latitude", DoubleType, false)))
    val originalPoints = sqlContext.createDataFrame(sc.emptyRDD[Row], struct)
    val newPoints = sqlContext.createDataFrame(sc.emptyRDD[Row], struct)

    val pointRecall = new PointRecall()
      .setReferenceFrame(originalPoints)
      .setTraceIdColumn("trajectoryId")
      .setLongitudeColumn("longitude")
      .setLatitudeColumn("latitude")
      .setDistanceThreshold(1000)

    /** apply the recall test to compare points of original with perturbed data */
    val results = pointRecall.compare(newPoints)

    val expectedColumns =
      List("trajectoryId", "_reference_points", "_comparison_points", "_recall")
    assert(results.select("trajectoryId").count === 0)
    assert(results.columns.forall(expectedColumns.contains))

    val emptyResult = results.groupBy(lit(1)).agg(mean("_recall")).first
    assert(Option(emptyResult(1)).isEmpty)
  }

  test("no recall - empty comparison frame") {
    val sqlContext = new SQLContext(this.sc)
    import sqlContext.implicits._

    val struct =
      StructType(Array(
        StructField("trajectoryId", IntegerType, true),
        StructField("longitude", DoubleType, false),
        StructField("latitude", DoubleType, false)))
    val originalPoints = sqlContext.createDataFrame(sc.emptyRDD[Row], struct)

    val newPoints = Seq(
      ("1", "103.0001", "1.3501"),
      ("1", "103.0001", "1.3501"),
      ("1", "103.0001", "1.3501"),
      ("1", "103.0031", "1.3531"),
      ("2", "103.0001", "1.3501"),
      ("2", "102.0091", "1.3491"),
      ("2", "102.0081", "1.3481"),
      ("2", "102.0081", "1.3481"))
      .map(data => (data._1, data._2.toDouble, data._3.toDouble))
      .toDF("trajectoryId", "longitude", "latitude")

    val pointRecall = new PointRecall()
      .setReferenceFrame(originalPoints)
      .setTraceIdColumn("trajectoryId")
      .setLongitudeColumn("longitude")
      .setLatitudeColumn("latitude")
      .setDistanceThreshold(1000)

    /** apply the recall test to compare points of original with perturbed data */
    val results = pointRecall.compare(newPoints)

    val expectedColumns =
      List("trajectoryId", "_reference_points", "_comparison_points", "_recall")
    assert(results.select("trajectoryId").count === 0)
    assert(results.columns.forall(expectedColumns.contains))

    val emptyResult = results.groupBy(lit(1)).agg(mean("_recall")).first
    assert(Option(emptyResult(1)).isEmpty)
  }

  test("no recall - empty reference frame") {
    val sqlContext = new SQLContext(this.sc)
    import sqlContext.implicits._

    val originalPoints = Seq(
      ("1", "103.000", "1.350"),
      ("1", "103.001", "1.351"),
      ("1", "103.002", "1.352"),
      ("1", "103.003", "1.353"),
      ("2", "103.000", "1.350"),
      ("2", "102.009", "1.349"),
      ("2", "102.008", "1.348"),
      ("2", "102.007", "1.347"))
      .map(data => (data._1, data._2.toDouble, data._3.toDouble))
      .toDF("trajectoryId", "longitude", "latitude")

    val struct =
      StructType(Array(
        StructField("trajectoryId", IntegerType, true),
        StructField("longitude", DoubleType, false),
        StructField("latitude", DoubleType, false)))
    val newPoints = sqlContext.createDataFrame(sc.emptyRDD[Row], struct)

    val pointRecall = new PointRecall()
      .setReferenceFrame(originalPoints)
      .setTraceIdColumn("trajectoryId")
      .setLongitudeColumn("longitude")
      .setLatitudeColumn("latitude")
      .setDistanceThreshold(1000)

    /** apply the recall test to compare points of original with perturbed data */
    val results = pointRecall.compare(newPoints)

    val expectedColumns =
      List("trajectoryId", "_reference_points", "_comparison_points", "_recall")
    assert(results.select("trajectoryId").count === 0)
    assert(results.columns.forall(expectedColumns.contains))

    val emptyResult = results.groupBy(lit(1)).agg(mean("_recall")).first
    assert(Option(emptyResult(1)).isEmpty)
  }
}
