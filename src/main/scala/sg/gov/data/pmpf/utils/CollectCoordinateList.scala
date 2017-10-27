/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.utils

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class CollectCoordinateList extends UserDefinedAggregateFunction {
  def inputSchema: StructType = new StructType()
    .add("longitude", DoubleType)
    .add("latitude", DoubleType)

  def bufferSchema: StructType = new StructType()
    .add("longitudes", ArrayType(DoubleType))
    .add("latitudes", ArrayType(DoubleType))

  def dataType: DataType = ArrayType(
    StructType(StructField("longitude", DoubleType) :: StructField("latitude", DoubleType) :: Nil)
  )

  def deterministic: Boolean = false

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    // initialise longitude buffer
    buffer(0) = Array[Double]()
    // initialise latitude buffer
    buffer(1) = Array[Double]()
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // update longitude buffer
    buffer(0) = buffer.getSeq(0) :+ input.getAs[Double](0)
    // update latitude buffer
    buffer(1) = buffer.getSeq(1) :+ input.getAs[Double](1)
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // merge longitude buffers
    buffer1(0) = buffer1.getSeq(0) ++ buffer2.getSeq(0)
    // merge latitude buffers
    buffer1(1) = buffer1.getSeq(1) ++ buffer2.getSeq(1)
  }

  def evaluate(buffer: Row): Any = {
    buffer.getSeq(0) zip buffer.getSeq(1)
  }
}
