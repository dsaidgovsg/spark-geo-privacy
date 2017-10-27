/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf

// scalastyle:off funsuite
import org.scalatest.{FunSuite, Outcome}

/*
 * Log the suite name and the test name before and after each test.
 *
 * Subclasses should never override this method. If they wish to run
 * custom code before and after each test, they should mix in the
 * {{org.scalatest.BeforeAndAfter}} trait instead.
 */
abstract class SparkFunSuite extends FunSuite with Logging {
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }
}
