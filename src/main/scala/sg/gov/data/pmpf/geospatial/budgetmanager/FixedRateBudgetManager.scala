/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial.budgetmanager

import sg.gov.data.pmpf.geospatial.TraceGeoI._

/*
 * Figure 2d in the paper.
 */
class FixedRateBudgetManager extends BudgetManager
  with HasBudgetRate
  with HasPredictionRate
  with HasTestUtilityFactor
  with HasTestNoiseThresholdRatio {

  override def getBudgetConfig(reportedTrace: Seq[ReportedStep]): BudgetConfig = {
    val noiseBudget = budgetRate / ((1-predictionRate) +
      laplace90PercentileBound / planarLaplace90PercentileRadius *
        testUtilityFactor * (1 + 1 / testNoiseThresholdRatio))

    val testBudget = noiseBudget * testUtilityFactor *
      (laplace90PercentileBound / planarLaplace90PercentileRadius) *
      (1 + 1 / testNoiseThresholdRatio)

    val testThreshold = laplace90PercentileBound / (testNoiseThresholdRatio * testBudget)

    BudgetConfig(testBudget, testThreshold, noiseBudget)
  }
}
