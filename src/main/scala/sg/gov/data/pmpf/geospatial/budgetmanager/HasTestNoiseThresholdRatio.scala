/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial.budgetmanager

/*
 * Ratio of test noise utility to test threshold.
 * Should take values between 0 and 1, inclusive.
 * Referred to as `gamma` in the paper.
 */
trait HasTestNoiseThresholdRatio {
  var testNoiseThresholdRatio: Double = 0.7

  def setTestNoiseThresholdRatio(newTestNoiseThresholdRatio: Double): this.type = {
    testNoiseThresholdRatio = newTestNoiseThresholdRatio
    this
  }
}
