/*
 * spark-geo-privacy: Geospatial privacy functions for Apache Spark
 * Copyright (C) 2017 Government Technology Agency of Singapore <https://www.tech.gov.sg>
 */

// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

// setting spark version to match production for now
val sparkVer = sys.props.getOrElse("spark.version", "1.6.3")

val sparkBranch = sparkVer.substring(0, 3)
val defaultScalaVer = sparkBranch match {
  case "1.6" => "2.10.6"
  case "2.0" => "2.11.8"
  case "2.1" => "2.11.8"
  case _ => throw new IllegalArgumentException(s"Unsupported Spark version: $sparkVer.")
}
val scalaVer = sys.props.getOrElse("scala.version", defaultScalaVer)

val defaultScalaTestVer = scalaVer match {
  case s if s.startsWith("2.10") => "2.0"
  case s if s.startsWith("2.11") => "2.2.6" // scalatest_2.11 does not have 2.0 published
}

sparkVersion := sparkVer

scalaVersion := scalaVer

spName := "datagovsg/spark-geo-privacy"

// Don't forget to set the version
version := "0.1.3"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// Add Spark components this package depends on
sparkComponents ++= Seq("sql", "hive")

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % defaultScalaTestVer % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3" % "test"

// These versions are ancient, but they cross-compile around scala 2.10 and 2.11.
// Update them when dropping support for scala 2.10
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

// java libraries
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
libraryDependencies += "org.locationtech.spatial4j" % "spatial4j" % "0.6"

resolvers += Resolver.bintrayRepo("mskimm", "maven")
libraryDependencies += "com.github.mskimm" %% "ann4s" % "0.0.6"

// disable tests when running assembly
test in assembly := {}

// disable parallel test execution to avoid SparkSession conflicts
parallelExecution in Test := false

// run the Test tasks in own JVM separate from sbt's JVM
//fork in Test := true

// Style check test sources when running tests
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value
(test in Test) := ((test in Test) dependsOn testScalastyle).value
