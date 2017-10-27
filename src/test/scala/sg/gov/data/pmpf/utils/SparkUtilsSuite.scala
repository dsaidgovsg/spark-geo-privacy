/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.utils

import sg.gov.data.pmpf.{SparkFunSuite, TestSparkContext}

import org.apache.spark.sql.hive.test.TestHiveContext

class SparkUtilsSuite extends SparkFunSuite
  with TestSparkContext {

  test("sample df by columns") {
    implicit val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._
    val sampleDf = Seq(10, 100, 1000).flatMap(x => (0 until x).map((x, _, x)))
      .toDF("id", "value1", "value2")

    val sampleDf5 = SparkUtils.sampleDfByCol(sampleDf, "id", 5)
    assert(sampleDf5.columns.sameElements(sampleDf.columns))
    assert(sampleDf5.count === 15)
    sampleDf5.groupBy("id").count.collect
      .foreach(row => assert(row.getAs[Int]("count") === 5))

    val sampleDf50 = SparkUtils.sampleDfByCol(sampleDf, "id", 50)
    val expectedSampleDf50 = Map(10 -> 10, 100 -> 50, 1000 -> 50)
    assert(sampleDf50.columns.sameElements(sampleDf.columns))
    assert(sampleDf50.count === 110)
    sampleDf50.groupBy("id").count.collect
      .foreach(row => assert(row.getAs[Int]("count") === expectedSampleDf50(row.getAs[Int]("id"))))

    val sampleDf500 = SparkUtils.sampleDfByCol(sampleDf, "id", 500)
    val expectedSampleDf500 = Map(10 -> 10, 100 -> 100, 1000 -> 500)
    assert(sampleDf500.columns.sameElements(sampleDf.columns))
    assert(sampleDf500.count === 610)
    sampleDf500.groupBy("id").count.collect
      .foreach(row => assert(row.getAs[Int]("count") === expectedSampleDf500(row.getAs[Int]("id"))))
  }

  test ("split df by columns") {
    implicit val hiveContext = new TestHiveContext(this.sc)
    import hiveContext.implicits._

    val columnNames = (0 until 10).map(_.toString).toArray
    val df = (0 until 10)
      .map(x => (x, x + 1, x + 2, x + 3, x + 4, x + 5, x + 6, x + 7, x + 8, x + 9))
      .toDF(columnNames: _*)
    val collectedDf = df.orderBy("0").collect

    // select some columns
    val selectedCols = (0 until 5).map(_.toString).toArray
    val otherCols = columnNames.diff(selectedCols)
    val (selectedDf, otherDf) = SparkUtils.splitDfByCols(df, selectedCols, "row_id")
    assert(selectedDf.count === 10)
    assert(otherDf.count === 10)
    assert(selectedDf.columns.length === 6)
    assert(otherDf.columns.length === 6)
    assert(selectedDf.columns.sameElements(selectedCols :+ "row_id"))
    assert(otherDf.columns.sameElements(otherCols :+ "row_id"))

    // join back split dfs
    val joinedDf = selectedDf.join(otherDf, selectedDf("row_id").equalTo(otherDf("row_id")))
      .drop("row_id")
    assert(joinedDf.count === 10)
    assert(joinedDf.columns.length === 10)
    assert(joinedDf.columns.sorted.sameElements(columnNames))
    assert(joinedDf.orderBy("0").collect.sameElements(collectedDf))

    // select all columns
    val (selectedAllDf, otherEmpDf) = SparkUtils.splitDfByCols(df, columnNames)
    assert(selectedAllDf.count === 10)
    assert(otherEmpDf.count === 10)
    assert(selectedAllDf.columns.length === 11)
    assert(otherEmpDf.columns.length === 1)
    assert(selectedAllDf.columns.sameElements(columnNames :+ "_row_id"))
    assert(otherEmpDf.columns.sameElements(Array("_row_id")))
    assert(selectedAllDf.drop("_row_id").orderBy("0").collect.sameElements(collectedDf))

    // select no columns
    val (selectedEmpDf, otherAllDf) = SparkUtils.splitDfByCols(df, Array())
    assert(selectedEmpDf.count === 10)
    assert(otherAllDf.count === 10)
    assert(selectedEmpDf.columns.length === 1)
    assert(otherAllDf.columns.length === 11)
    assert(selectedEmpDf.columns.sameElements(Array("_row_id")))
    assert(otherAllDf.columns.sameElements(columnNames :+ "_row_id"))
    assert(otherAllDf.drop("_row_id").orderBy("0").collect.sameElements(collectedDf))
  }
}
