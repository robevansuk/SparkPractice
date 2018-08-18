package uk.robevans

import org.apache.spark.ml.linalg.{Matrices, Matrix}
import org.apache.spark.ml.linalg.Vectors.dense
import org.scalatest.{FunSpec, Matchers}

class ResilientDistributedDatasetTest extends FunSpec with Matchers {

  it("Matrices can be local or distributed across machines") {

    // A local matrix has integer-typed rows - it is stored on a single machine
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

    // then take all cols, transform it to a list and assert we get 2 columns of data.
    dm.colIter.toList should contain theSameElementsAs(List(
      dense(1.0, 3.0, 5.0),
      dense(2.0, 4.0, 6.0)
    ))
  }

}
