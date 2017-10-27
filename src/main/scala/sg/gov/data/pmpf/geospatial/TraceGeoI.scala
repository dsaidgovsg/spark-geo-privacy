/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

package sg.gov.data.pmpf.geospatial

import sg.gov.data.pmpf.geospatial.budgetmanager.BudgetManager
import sg.gov.data.pmpf.geospatial.TraceGeoI._
import sg.gov.data.pmpf.param._
import sg.gov.data.pmpf.utils.Distributions.{laplacePPF, planarLaplacePolarPPF}
import sg.gov.data.pmpf.utils.GeodesicMath.{getDestination, getDistance}
import sg.gov.data.pmpf.utils.GPSLog
import sg.gov.data.pmpf.utils.SparkUtils.splitDfByCols

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, collect_list, explode, udf}

/*
 * Geospatial mobility trace privacy using geo-indistinguishability.
 *
 * Based on work by K. Chatzikokolakis, C. Palamidessi and M. Stronati.
 * "A Predictive Differentially-Private Mechanism for Mobility Traces."
 * Proceedings of the 14th Privacy Enhancing Technologies Symposium. PETS, 2014.
 */
class TraceGeoI extends Model
  with HasTraceIdColumn
  with HasDateTimeColumn
  with HasLongitudeColumn
  with HasLatitudeColumn
  with HasRandomSeed {

  var budgetManager: BudgetManager = _

  def setBudgetManager(newBudgetManager: BudgetManager ): this.type = {
    budgetManager = newBudgetManager
    this
  }

  def transform(df: DataFrame): DataFrame = {
    val tempRowIDColumn = "_row_id"
    val tempRowIDTraceColumn = "%s_trace".format(tempRowIDColumn)
    val tempDateTimeTraceColumn = "_%s_trace".format(dateTimeColumn)
    val tempLongitudeTraceColumn = "_%s_trace".format(longitudeColumn)
    val tempLatitudeTraceColumn = "_%s_trace".format(latitudeColumn)
    val tempJitteredColumn = "_jittered"
    val tempJitteredTraceColumn = "%s_trace".format(tempJitteredColumn)
    val jitteredRowIDColumn = "%s.rowId".format(tempJitteredColumn)
    val jitteredDateTimeColumn = "%s.gpsLog.timestamp".format(tempJitteredColumn)
    val jitteredLongitudeColumn = "%s.gpsLog.longitude".format(tempJitteredColumn)
    val jitteredLatitudeColumn = "%s.gpsLog.latitude".format(tempJitteredColumn)

    val jitterTraceUDF = udf[Seq[ReportedStep], Seq[Long],
      Seq[java.sql.Timestamp], Seq[Double], Seq[Double],
      Seq[Double], Seq[Double], Seq[Double]]{
      (rowId, timestamp, longitude, latitude, rand0, rand1, rand2) =>
        val GPSTrace = (timestamp, longitude, latitude).zipped.toList
          .map(x => GPSLog(x._1, x._2, x._3))
        val randsTrace = Array(rand0.toArray, rand1.toArray, rand2.toArray).transpose
        val inputTrace = (rowId, GPSTrace, randsTrace).zipped.toList
          .map(x => InputStep(x._1, x._2, x._3))
          .sortBy(_.gpsLog.timestamp.getTime)

        jitterTrace(budgetManager.getBudgetConfig)(inputTrace)
      }

    // split df for relevant portions only
    val selectedCols = Array(traceIdColumn, dateTimeColumn, longitudeColumn, latitudeColumn)
    val (selectedDf, otherDf) = splitDfByCols(df, selectedCols, tempRowIDColumn)

    val dfWithRands = selectedDf
      .withColumn("_rand0", rand())
      .withColumn("_rand1", rand())
      .withColumn("_rand2", rand())

    val tracesDf = dfWithRands
      .groupBy(traceIdColumn)
      .agg(
        collect_list(col(tempRowIDColumn)).alias(tempRowIDTraceColumn),
        collect_list(col(dateTimeColumn)).alias(tempDateTimeTraceColumn),
        collect_list(col(longitudeColumn)).alias(tempLongitudeTraceColumn),
        collect_list(col(latitudeColumn)).alias(tempLatitudeTraceColumn),
        collect_list(col("_rand0")).alias("_rand0_trace"),
        collect_list(col("_rand1")).alias("_rand1_trace"),
        collect_list(col("_rand2")).alias("_rand2_trace")
      )

    val jitteredTracesDf = tracesDf
      .withColumn(tempJitteredTraceColumn,
        jitterTraceUDF(
          col(tempRowIDTraceColumn),
          col(tempDateTimeTraceColumn),
          col(tempLongitudeTraceColumn),
          col(tempLatitudeTraceColumn),
          col("_rand0_trace"),
          col("_rand1_trace"),
          col("_rand2_trace")
        )
      )

    val jitteredDf = jitteredTracesDf
      .withColumn(tempJitteredColumn, explode(col(tempJitteredTraceColumn)))
      .select(
        col(jitteredRowIDColumn).alias(tempRowIDColumn),
        col(traceIdColumn),
        col(jitteredDateTimeColumn).alias(dateTimeColumn),
        col(jitteredLongitudeColumn).alias(longitudeColumn),
        col(jitteredLatitudeColumn).alias(latitudeColumn)
      )

    val jitteredFullDf = if (otherDf.columns.length == 1) {
      // otherDf doesn't contain extra columns, so no point joining back
      jitteredDf
    } else {
      // join with otherDf as it contains extra columns
      jitteredDf.join(otherDf, jitteredDf(tempRowIDColumn).equalTo(otherDf(tempRowIDColumn)))
    }

    // making sure original dataframe and returned dataframe have columns in the same order
    jitteredFullDf.select(df.columns.head, df.columns.tail: _*)
  }

  private def jitterTrace(budgetFunction: Seq[ReportedStep] => BudgetConfig)(
    inputTrace: Seq[InputStep]): Seq[ReportedStep] = {

    // just parrot last predicted point
    def predictNext(reportedTrace: Seq[ReportedStep], truePos: GPSLog): GPSLog = {
      val parrotedGPSLog = reportedTrace.map(_.gpsLog).last
      GPSLog(truePos.timestamp, parrotedGPSLog.longitude, parrotedGPSLog.latitude)
    }

    // test distance of predicted position to true position
    // against a threshold with laplace randomness
    def testPrediction(budgetConf: BudgetConfig)(predictedPos: GPSLog,
                                                 truePos: GPSLog,
                                                 rand: Double): Boolean = {

      val budget = budgetConf.testBudget
      val threshold = budgetConf.testThreshold
      if (threshold.isNegInfinity) {
        true
      } else if (threshold.isPosInfinity) {
        false
      } else {
        val geoPredictedPos = (predictedPos.longitude, predictedPos.latitude)
        val geoTruePos = (truePos.longitude, truePos.latitude)
        val distance = getDistance(geoPredictedPos, geoTruePos)
        val randThreshold = if (budget > 0) laplacePPF(1 / budget)(rand) else 0
        distance > (threshold + randThreshold)
      }
    }

    // add planar laplace on true position
    def noiseMechanism(budgetConf: BudgetConfig)(
      truePos: GPSLog, rands: (Double, Double)): GPSLog = {

      val budget = budgetConf.noiseBudget
      val (distance, bearing) =
        if (budget > 0) planarLaplacePolarPPF(1 / budget)(rands) else (0.0, 0.0)
      val (noiseLng, noiseLat) = getDestination(
        (truePos.longitude, truePos.latitude), distance, bearing
      )
      GPSLog(truePos.timestamp, noiseLng, noiseLat)
    }

    def jitterStep(reportedTrace: Seq[ReportedStep], currentPos: InputStep): Seq[ReportedStep] = {
      val rowId = currentPos.rowId
      val truePos = currentPos.gpsLog
      // decompose rands
      val (testRand, noiseRands) = currentPos.rands match {
        case Seq(rand0, rand1, rand2) => (rand0, (rand1, rand2))
      }
      val budgetConf = budgetFunction(reportedTrace)

      val curReportedStep = if (reportedTrace.isEmpty) {
        // for 1st point in trace, just noise mechanism since cannot be predicted
        val noisePos = noiseMechanism(budgetConf)(truePos, noiseRands)
        ReportedStep(rowId, noisePos,
          StepOutput(testOutcome = true, budgetConf.noiseBudget),
          budgetConf)
      } else {
        // for 2nd point onwards
        val predictedPos = predictNext(reportedTrace, truePos)
        val hardPrediction = testPrediction(budgetConf)(predictedPos, truePos, testRand)
        if (!hardPrediction) {
          // if predicted position is within bounds, use it
          ReportedStep(rowId, predictedPos,
            StepOutput(hardPrediction, budgetConf.testBudget),
            budgetConf)
        } else {
          // if prediction is off, use noise mechanism on true position
          val noisePos = noiseMechanism(budgetConf)(truePos, noiseRands)
          ReportedStep(rowId, noisePos,
            StepOutput(hardPrediction, budgetConf.testBudget + budgetConf.noiseBudget),
            budgetConf)
        }
      }

      reportedTrace :+ curReportedStep
    }

    inputTrace.foldLeft(Seq.empty[ReportedStep])(jitterStep)
  }
}

object TraceGeoI {
  case class StepOutput(testOutcome: Boolean, budgetUsed: Double)
  case class BudgetConfig(testBudget: Double, testThreshold: Double, noiseBudget: Double)
  case class InputStep(rowId: Long, gpsLog: GPSLog, rands: Seq[Double])
  case class ReportedStep(rowId: Long,
                          gpsLog: GPSLog,
                          stepOut: StepOutput,
                          budgetConf: BudgetConfig)
}
