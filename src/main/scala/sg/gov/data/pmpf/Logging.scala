/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf

import com.typesafe.scalalogging.slf4j.LazyLogging

trait Logging extends LazyLogging {
  protected def logDebug(s: String): Unit = logger.debug(s)
  protected def logInfo(s: String): Unit = logger.info(s)
  protected def logTrace(s: String): Unit = logger.trace(s)
}
