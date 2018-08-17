package uk.robevans

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

class CsvExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    val userSchema = new StructType().add("one", "double")
    val csvDF = spark
      .readStream
      .option("header", "true")
      .option("sep", ",")
      .schema(userSchema)
      .csv("resources")

    val dataset: Dataset[Double] = csvDF.as[Double]

    dataset.createOrReplaceTempView("numbers")
    val wordsResult = spark.sql("select one from numbers") // returns another streaming DF

    val query = wordsResult.writeStream
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", "checkpoint")
      .option("path", "output")
      .start()

    query.awaitTermination()
  }
}
