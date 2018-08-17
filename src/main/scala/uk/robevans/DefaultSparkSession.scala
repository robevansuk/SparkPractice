package uk.robevans

import org.apache.spark.sql.SparkSession

trait DefaultSparkSession {
  println("Creating SparkSession")
  val spark = SparkSession
    .builder
    .appName("DFS Read Write Test")
    .config("spark.master", "local")
    .config("spark.hadoop.validateOutputSpecs", "false")
    .getOrCreate()
}
