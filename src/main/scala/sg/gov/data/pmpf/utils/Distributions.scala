/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.utils

import org.apache.commons.math3.distribution.{GammaDistribution, LaplaceDistribution}

object Distributions {
  def gammaPPF(scale: Double)(percentile: Double): Double = {
    val gammaDistribution = new GammaDistribution(2, scale)
    gammaDistribution.inverseCumulativeProbability(percentile)
  }

  def laplacePPF(scale: Double)(percentile: Double): Double = {
    val laplaceDistribution = new LaplaceDistribution(0, scale)
    laplaceDistribution.inverseCumulativeProbability(percentile)
  }

  def planarLaplacePolarPPF(scale: Double)(rands: (Double, Double)): (Double, Double) = {
    val (rand1, rand2) = rands
    val distance = gammaPPF(scale)(rand1)
    val bearing = rand2 * 360
    (distance, bearing)
  }
}
