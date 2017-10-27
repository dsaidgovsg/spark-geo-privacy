/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial

import java.sql.Timestamp

import scala.annotation.tailrec

import sg.gov.data.pmpf.param.{HasDateTimeColumn, HasLatitudeColumn, HasLongitudeColumn, HasTraceIdColumn}
import sg.gov.data.pmpf.utils.GeodesicMath.{getDistance, getMeanCoordinate}
import sg.gov.data.pmpf.utils.GPSLog

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, collect_list, explode, udf}

trait DistanceThreshold {
  var distanceThreshold: Double = 200.0

  def setDistanceThreshold(newDistanceThreshold: Double): this.type = {
    distanceThreshold = newDistanceThreshold
    this
  }
}

trait TimeThreshold {
  var timeThreshold: Double = 30 * 60

  def setTimeThreshold(newTimeThreshold: Double): this.type = {
    timeThreshold = newTimeThreshold
    this
  }
}

/*
 * Extract points of interest from GPS traces using the stay point detection
 * algorithm.
 *
 * Based on work by Q. Li, et al. "Mining user similarity based on location
 * history", in Proceedings of the 16th ACM SIGSPATIAL international conference
 * on Advances in geographic information systems, New York, NY, USA, 2008,
 * pp. 34:1--34:10.
 */
class SPD extends Model
  with HasTraceIdColumn
  with HasLongitudeColumn
  with HasLatitudeColumn
  with HasDateTimeColumn
  with DistanceThreshold
  with TimeThreshold {

  private case class StayPoint(
      longitude: Double,
      latitude: Double,
      arriveTime: Timestamp,
      departTime: Timestamp)

  @tailrec
  private def spdAccum(accum: Seq[StayPoint],
                       j: Int,
                       i: Int,
                       trace: Seq[GPSLog]
                      ): (Seq[StayPoint], Int, Boolean) = {
    if (j >= trace.size) {
      (accum, i + 1, false)
    } else {
      val pointI: GPSLog = trace(i)
      val pointJ: GPSLog = trace(j)

      val dist =
        getDistance((pointI.longitude, pointI.latitude), (pointJ.longitude, pointJ.latitude))
      if (dist > distanceThreshold) {
        val arriveTimeInSeconds = pointI.timestamp.getTime / 1000L
        val departTimeInSeconds = pointJ.timestamp.getTime / 1000L

        if ((departTimeInSeconds - arriveTimeInSeconds) > timeThreshold) {
          val subPoints = trace.slice(i, j + 1)
          val (longitude, latitude) =
            getMeanCoordinate(subPoints.map(i => (i.longitude, i.latitude)))
          (accum :+ StayPoint(longitude, latitude, pointI.timestamp, pointJ.timestamp), j, true)
        } else {
          (accum, i, false)
        }
      } else {
        spdAccum(accum, j + 1, i, trace)
      }
    }
  }

  @tailrec
  private def spd(accum: Seq[StayPoint], i: Int, trace: Seq[GPSLog]): Seq[StayPoint] = {
    if (i >= trace.size) {
      accum
    } else {
      val partialAccum = spdAccum(accum, i + 1, i, trace)
      if (partialAccum._3) {
        spd(partialAccum._1, partialAccum._2, trace)
      } else {
        spd(partialAccum._1, i + 1, trace)
      }
    }
  }

  def extract(df: DataFrame): DataFrame = {

    val dfSorted = df.sort(asc(dateTimeColumn))
    val dfTraced = dfSorted.groupBy(traceIdColumn)
      .agg(collect_list(latitudeColumn).alias(latitudeColumn),
        collect_list(longitudeColumn).alias(longitudeColumn),
        collect_list(dateTimeColumn).alias(dateTimeColumn))

    val udfSPD = udf[Seq[StayPoint], Seq[Double], Seq[Double], Seq[Timestamp]]((lat, lng, dt) => {
      val points = (dt, lng, lat)
        .zipped
        .map((dt, lng, lat) => GPSLog(dt, lng, lat))
        .sortBy(_.timestamp.getTime) /** To ensure trace in chronologically ascending order */
      spd(Seq.empty[StayPoint], 0, points)
    })

    val dfSPD = dfTraced.withColumn("sp", udfSPD(col(latitudeColumn),
      col(longitudeColumn),
      col(dateTimeColumn)))
    val dfSPDEx = dfSPD.withColumn("sp_explode", explode(col("sp")))

    dfSPDEx.select(col(traceIdColumn),
      col("sp_explode.longitude").alias(longitudeColumn),
      col("sp_explode.latitude").alias(latitudeColumn),
      col("sp_explode.arriveTime"),
      col("sp_explode.departTime"))
  }
}
