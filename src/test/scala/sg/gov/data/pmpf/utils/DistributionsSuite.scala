/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.utils

import org.scalactic.Tolerance
import sg.gov.data.pmpf.SparkFunSuite

class DistributionsSuite extends SparkFunSuite with Tolerance {

  private val params = Seq(2, 5).flatMap(x => Seq(0.1, 0.5, 0.9).map((x, _)))

  test("gamma PPF") {
    Seq(1.063623216578875, 3.3566939800333233, 7.7794403394369995,
    2.6590580424472403, 8.391734950083308, 19.44860084959289).zip(params)
      .foreach{case (v, (scale, p)) =>
        assert(Distributions.gammaPPF(scale)(p) === v +- 0.000000001)}
  }

  test("laplace PPF") {
    Seq(-3.2188758248682006, 0.0, 3.218875824868201,
    -8.047189562170502, 0.0, 8.047189562170502).zip(params)
      .foreach{case (v, (scale, p)) =>
        assert(Distributions.laplacePPF(scale)(p) === v +- 0.000000001)}
  }

  test("planar laplace PPF") {
    Seq(1.063623216578875, 3.3566939800333233, 7.7794403394369995,
      2.6590580424472403, 8.391734950083308, 19.44860084959289).zip(params)
      .foreach{case (v, (scale, p)) =>
        Distributions.planarLaplacePolarPPF(scale)(p, p)._1 === v +- 0.000000001}

    Seq(36.0, 180.0, 324.0, 36.0, 180.0, 324.0).zip(params)
      .foreach{case (v, (scale, p)) =>
        Distributions.planarLaplacePolarPPF(scale)(p, p)._2 === v +- 0.000000001}
  }
}
