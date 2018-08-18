package uk.robevans

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}

class DataTypeTesting extends FunSpec with Matchers {

  val spark = SparkSession.builder().config("spark.master", "local(2)").getOrCreate()

  it("RDD") {
    val rdd: RDD[String] = spark.sparkContext.parallelize(Array("a", "b", "c", "d"))

    // when
    val result = rdd.map(_.toUpperCase).collect.toList

    result should contain theSameElementsAs List("A", "B", "C", "D")
  }

  it("Should be able to create a dataframe from RDD") {
    import spark.sqlContext.implicits._

    // create a dataframe - data frame has no type - we don't know what data will be inside.
    // We have to look at the data internally to find out
    val userData = spark.sparkContext.makeRDD(List(
      User("Rob", 33),
      User("John", 34),
      User("Ed", 40)
    )).toDF()
    // need this to query the data.. we don't query the actual data, but a temp view of it
    userData.createOrReplaceTempView("user_data")

    // when
    val userDataForUserIdRob = userData.where("name = 'Rob'").count()

    userDataForUserIdRob should be (1)
  }

  it("Should be able to create a dataset") {
    import spark.sqlContext.implicits._

    // create a *dataset* - this is typed and we can get compile time checking.
    val userData = spark.sparkContext.makeRDD(List(
      User("Rob", 33),
      User("John", 34),
      User("Ed", 40)
    )).toDS()

    // need this to query the data.. we don't query the actual data, but a temp view of it
    userData.createOrReplaceTempView("user_data")

    // when
    val userDataForUserIdJohn = userData.filter(_.name == "John").count()

    userDataForUserIdJohn should be (1)
  }

  /**
    * labelled data is used for supervised learning (we know the result we want)
    * unlabelled data is used for unsupervised learning (the algorithm should do the work)
    */
  it("Labelled points are used in supervised learning. We should be able to create them") {
    /**
      * We can rely on positively labelled points
      */
    //First arg is the result we want (true/yes/match = 1, false/no/no match = 0)
    // this is a positively matching label
    val positiveLabelledPoint = LabeledPoint(1.0, dense(1.0, 0.0, 3.0))

    // Here this is a non matching point - it uses a sparse vector - this is the same as a dense matrix
    // but this time we specify the size of the matrix, the array indexes we will supply
    // and finally another array with the values at those indices.
    val negativeLabelledPoint = LabeledPoint(0.0, sparse(3, Array(0, 2), Array(1.0, 3.0)))

    // then
    positiveLabelledPoint.label should be (1.0)
    negativeLabelledPoint.label should be (0.0)
  }
}


case class User(name: String, age: Int)