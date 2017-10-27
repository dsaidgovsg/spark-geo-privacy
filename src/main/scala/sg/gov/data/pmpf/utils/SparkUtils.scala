/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, rand}
import org.apache.spark.sql.hive.HiveContext

object SparkUtils {
  def sampleDfByCol(df: DataFrame, idCol: String, n: Int = 100)(implicit hiveContext: HiveContext)
    : DataFrame = {
    val RandColName = "_rand"
    val dfWithRand = df.withColumn(RandColName, rand)
    val schema = dfWithRand.schema
    val idColIdx = dfWithRand.columns.indexOf(idCol)

    val sampledRDD = dfWithRand.rdd.keyBy(_.get(idColIdx))
      .groupByKey
      .mapValues(_.toSeq)
      .flatMapValues{x =>
        if (x.length > n) {
          x.sortBy(_.getAs[Double](RandColName)).take(n)
        } else {
          x
        }
      }
      .values
    val sampledDf = hiveContext.createDataFrame(sampledRDD, schema)
    sampledDf.drop(col(RandColName))
  }

  def splitDfByCols(df: DataFrame, selectedCols: Array[String], rowIDCol: String = "_row_id")
    : (DataFrame, DataFrame) = {
    val dfWithId = df.withColumn(rowIDCol, monotonically_increasing_id())
    val metaCols = df.columns.diff(selectedCols)
    val dfSelected = metaCols.foldLeft(dfWithId)((df, colName) => df.drop(colName))
    val dfOther = selectedCols.foldLeft(dfWithId)((df, colName) => df.drop(colName))
    // dfs can be joined back together by
    // dfSelected
    //   .join(otherDf, dfSelected(rowIDCol).equalTo(otherDf(rowIDCol)))
    //   .drop(rowIDCol)
    (dfSelected, dfOther)
  }
}
