/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial.budgetmanager

/*
 * Discount factor for test utility.
 * Should take values between 0 and 1, inclusive.
 * Referred to as `eta` in the paper.
 */
trait HasTestUtilityFactor {
  var testUtilityFactor: Double = 0.7

  def setTestUtilityFactor(newTestUtilityFactor: Double): this.type = {
    testUtilityFactor = newTestUtilityFactor
    this
  }
}
