/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial.budgetmanager

/* Referred to as `rho` in the paper. */
trait HasBudgetRate {
  var budgetRate: Double = 0.01

  def setBudgetRate(newBudgetRate: Double): this.type = {
    budgetRate = newBudgetRate
    this
  }
}
