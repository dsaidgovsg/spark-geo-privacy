/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial

import ann4s._
import sg.gov.data.pmpf.geospatial.types._
import sg.gov.data.pmpf.param._
import sg.gov.data.pmpf.utils.CollectCoordinateList
import sg.gov.data.pmpf.utils.GeodesicMath.getDistance

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, udf}

/*
 * Metric for evaluating point recall after privacy transformations.
 *
 * Based on work by Primault, Vincent, et al. "Time Distortion Anonymization
 * for the Publication of Mobility Data with High Utility."
 * Trustcom/BigDataSE/ISPA, 2015 IEEE. Vol. 1. IEEE, 2015.
 */
class PointRecall extends Model
  with HasTraceIdColumn
  with HasLongitudeColumn
  with HasLatitudeColumn
  with HasDistanceThreshold
  with HasReferenceFrame {

  private def makeAnnModel(dim: Int) = new AnnoyIndex(dim, Euclidean)

  implicit class AnnoyModel(annoyModel: AnnoyIndex) {
    def trainWith(trainingPoints: Seq[GeoPoint]): AnnoyIndex = {
      trainingPoints.map(AnnPoint3D).zipWithIndex
        .foreach(point => annoyModel.addItem(point._2, point._1))
      annoyModel.build(1)
      annoyModel
    }

    def findNearest(queryPoints: Seq[GeoPoint]): Seq[Int] =
      queryPoints.map(AnnPoint3D).map(point => annoyModel.getNnsByVector(point, 1).head._1)
  }

  implicit class GeoPoints(geoPoints: Seq[GeoPoint]) {
    def zipWithNearestNeighbourFrom(queryPoints: Seq[GeoPoint]): Seq[(GeoPoint, GeoPoint)] = {
      val neighbourIndices: Seq[Int] = makeAnnModel(3).trainWith(queryPoints).findNearest(geoPoints)
      val matchedQueryPoints: Seq[GeoPoint] = neighbourIndices.collect(queryPoints)
      geoPoints.zip(matchedQueryPoints)
    }
  }

  private def withinDistanceThreshold(points: (GeoPoint, GeoPoint)) =
    getDistance(points._1, points._2) < distanceThreshold

  private def getRecall(referencePoints: Seq[GeoPoint], comparisonPoints: Seq[GeoPoint]) = {
    val matchedPoints: Seq[(GeoPoint, GeoPoint)] =
      referencePoints.zipWithNearestNeighbourFrom(comparisonPoints).filter(withinDistanceThreshold)

    matchedPoints.map(_._2).distinct.length.toDouble / referencePoints.length.toDouble
  }

  def compare(comparisonFrame: DataFrame,
              recallCol: String = "_recall",
              referencePointsCol: String = "_reference_points",
              comparisonPointsCol: String = "_comparison_points"
             ): DataFrame = {
    val collectAsTuple = new CollectCoordinateList

    val referenceVectors = referenceFrame
      .groupBy(traceIdColumn)
      .agg(collectAsTuple(col(longitudeColumn), col(latitudeColumn)).as(referencePointsCol))

    val comparisonVectors = comparisonFrame
      .groupBy(traceIdColumn)
      .agg(collectAsTuple(col(longitudeColumn), col(latitudeColumn)).as(comparisonPointsCol))

    val allVectors = referenceVectors.join(comparisonVectors, traceIdColumn)

    val udfRecall = udf(
      (referencePoints: Seq[Row], comparisonPoints: Seq[Row]) => getRecall(
        referencePoints.map(point => (point.getDouble(0), point.getDouble(1))),
        comparisonPoints.map(point => (point.getDouble(0), point.getDouble(1)))
      )
    )

    allVectors.withColumn(recallCol, udfRecall(col(referencePointsCol), col(comparisonPointsCol)))
  }
}
