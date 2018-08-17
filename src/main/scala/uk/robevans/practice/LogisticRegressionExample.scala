package uk.robevans.practice

import java.io.File

import org.apache.spark.ml.classification.LogisticRegression
import uk.robevans.DefaultSparkSession

object LogisticRegressionExample extends DefaultSparkSession {

  private var localFile: File = new File(ClassLoader.getSystemResource("data/mllib/xor.txt").toURI())
  private var dfsDirPath: String = ClassLoader.getSystemResource("").toURI().toString()

  def main(args: Array[String]) = {

    val training = spark.read.format("libsvm").load(localFile.getAbsolutePath)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // We can also use the multinomial family for binary classification
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")
  }
}
