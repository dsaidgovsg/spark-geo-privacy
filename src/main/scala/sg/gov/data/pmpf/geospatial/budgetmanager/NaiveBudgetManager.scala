/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial.budgetmanager

import sg.gov.data.pmpf.geospatial.TraceGeoI._

class NaiveBudgetManager extends BudgetManager
  with HasBudgetRate {

  override def getBudgetConfig(reportedTrace: Seq[ReportedStep]): BudgetConfig = {
    BudgetConfig(0, Double.NegativeInfinity, budgetRate)
  }
}

