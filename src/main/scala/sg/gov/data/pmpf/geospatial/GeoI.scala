/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial

import sg.gov.data.pmpf.param._
import sg.gov.data.pmpf.utils.Distributions.gammaPPF
import sg.gov.data.pmpf.utils.GeodesicMath.getDestination

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}

/*
 * Geospatial mobility trace privacy using geo-indistinguishability.
 *
 * Based on work by Andres, Miguel E., et al. "Geo-indistinguishability:
 * Differential privacy for location-based systems." Proceedings of the 2013
 * ACM SIGSAC conference on Computer & communications security. ACM, 2013.
 */
class GeoI extends Model
  with HasScale
  with HasLongitudeColumn
  with HasLatitudeColumn
  with HasRandomSeed {

  def transform(df: DataFrame): DataFrame = {
    val tempDistanceColumn = "_distance"
    val tempBearingColumn = "_bearing"
    val tempPositionColumn = "_position"

    val udfGammaPPF = udf[Double, Double](gammaPPF(scale)(_))
    val udfAddPosition = udf[Seq[Double], Double, Double, Double, Double](
      (originLon, originLat, distance, bearing) => {
        val (destLon, destLat) = getDestination((originLon, originLat), distance, bearing)
        Seq(destLon, destLat)
      }
    )

    val polarDF = df
      .withColumn(tempDistanceColumn, rand())
      .withColumn(tempDistanceColumn, udfGammaPPF(col(tempDistanceColumn)))
      .withColumn(tempBearingColumn, rand() * 360)

    val jitteredDF = polarDF
      .withColumn(tempPositionColumn,
        udfAddPosition(
          col(longitudeColumn),
          col(latitudeColumn),
          col(tempDistanceColumn),
          col(tempBearingColumn)
        )
      )
      .withColumn(longitudeColumn, col(tempPositionColumn)(0))
      .withColumn(latitudeColumn, col(tempPositionColumn)(1))
      .drop(tempPositionColumn)
      .drop(tempDistanceColumn)
      .drop(tempBearingColumn)

    jitteredDF
  }
}
