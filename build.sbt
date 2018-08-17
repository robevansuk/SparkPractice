name := "SparkPractice"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions ++= Seq("-unchecked",
  "-deprecation",
  "-feature",
  "-Xfatal-warnings",
  "-Xlint:_,-missing-interpolator",
  "-Ypartial-unification")

lazy val ApacheSpark = Seq(
  "org.apache.spark" %% "spark-core"  % "2.3.1",
  "org.apache.spark" %% "spark-sql"   % "2.3.1",
  "org.apache.spark" %% "spark-mllib" % "2.3.1"
)

libraryDependencies ++= ApacheSpark