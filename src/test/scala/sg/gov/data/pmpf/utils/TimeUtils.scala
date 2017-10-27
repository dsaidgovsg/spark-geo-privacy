/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.utils

import java.sql.Timestamp

import org.joda.time.Instant

object TimeUtils {
  def fromISO(timestamp: String): Timestamp = {
    new Timestamp(Instant.parse(timestamp).getMillis())
  }
}
