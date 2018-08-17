package uk.robevans

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source._

object CsvExample {

  private var localFilePath: File = new File(ClassLoader.getSystemResource("test.csv").toURI())
  private var dfsDirPath: String = ClassLoader.getSystemResource("").toURI().toString()

  private def readFile(filename: String): List[String] = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }

  def runLocalWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }

  def overwriteAnyExistingFile(dfsFilename: String, fileRDD: RDD[String]) = {
    val filePath = new File(dfsFilename).getAbsolutePath()
    Runtime.getRuntime.exec(s"rm ${filePath}")
    fileRDD.saveAsTextFile(dfsFilename)
  }

  def main(args: Array[String]): Unit = {

    val fileContents = readFile(localFilePath.toString())
    val localWordCount = runLocalWordCount(fileContents)

    println("Creating SparkSession")
    val spark = SparkSession
      .builder
      .appName("DFS Read Write Test")
      .config("spark.master", "local")
      .getOrCreate()

    println("Writing local file to DFS")
    val dfsFilename = s"$dfsDirPath/dfs_read_write_test"
    val fileRDD: RDD[String] = spark.sparkContext.parallelize(fileContents)
    overwriteAnyExistingFile(dfsFilename, fileRDD)

    println("Reading file from DFS and running Word Count")
    val readFileRDD = spark.sparkContext.textFile(dfsFilename)

    val dfsWordCount = readFileRDD
      .flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .map(w => (w, 1))
      .countByKey()
      .values
      .sum

    spark.stop()

    if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count $localWordCount and " +
        s"DFS Word Count $dfsWordCount agree.")
    } else {
      println(s"Failure! Local Word Count $localWordCount " +
        s"and DFS Word Count $dfsWordCount disagree.")
    }
  }
}
