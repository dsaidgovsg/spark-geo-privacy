/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial.budgetmanager

/*
 * Prediction rate of prediction function.
 * Referred to as `PR` in the paper.
 */
trait HasPredictionRate {
  var predictionRate: Double = 0.5

  def setPrediction(newPredictionRate: Double): this.type = {
    predictionRate = newPredictionRate
    this
  }
}
