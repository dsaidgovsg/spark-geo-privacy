/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial.budgetmanager

import sg.gov.data.pmpf.geospatial.TraceGeoI._

/*
 * Figure 2c in the paper.
 */
class FixedUtilityBudgetManager extends BudgetManager
  with HasUtilityRate
  with HasTestUtilityFactor
  with HasTestNoiseThresholdRatio {

  override def getBudgetConfig(reportedTrace: Seq[ReportedStep]): BudgetConfig = {
    val testBudget = testUtilityFactor *
      laplace90PercentileBound /
      utility *
      (1 + 1 / testNoiseThresholdRatio)
    val noiseBudget = planarLaplace90PercentileRadius / utility
    val testThreshold = laplace90PercentileBound / (testNoiseThresholdRatio * testBudget)
    BudgetConfig(testBudget, testThreshold, noiseBudget)
  }
}

