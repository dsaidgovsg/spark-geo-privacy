/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial.budgetmanager

/*
 * Referred to as `alpha` in the paper.
 */
trait HasUtilityRate {
  var utility: Double = 100

  def setUtility(newUtility: Double): this.type = {
    utility = newUtility
    this
  }
}
