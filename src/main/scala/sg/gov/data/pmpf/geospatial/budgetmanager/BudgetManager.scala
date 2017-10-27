/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial.budgetmanager

import sg.gov.data.pmpf.geospatial.TraceGeoI._
import sg.gov.data.pmpf.utils.Distributions.{gammaPPF, laplacePPF}

abstract class BudgetManager extends Serializable {

  def getBudgetConsumed(reportedTrace: Seq[ReportedStep]): Double = {
    reportedTrace.map(_.stepOut.budgetUsed).sum
  }

  def getBudgetConfig(reportedTrace: Seq[ReportedStep]): BudgetConfig

  // Referred to as `c_theta` in the paper.
  // Gives bounds where probability within bounds of laplace distribution is 0.9.
  protected val laplace90PercentileBound: Double = laplacePPF(1)(0.95)

  // Referred to as `c_N` in the paper.
  // Gives radius where probability within radius of planar laplace distribution is 0.9.
  protected val planarLaplace90PercentileRadius: Double = gammaPPF(1)(0.9)
}
