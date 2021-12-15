package org.apache.spark.ml.made

import breeze.linalg.{*, DenseMatrix, DenseVector}
import breeze.stats.distributions.RandBasis
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class LinRegTest
  extends AnyFlatSpec
    with should.Matchers
{
  lazy val data: DataFrame = LinRegTest._data

  lazy val model: DenseVector[Double] = LinRegTest._weights

  "Params" should "contains" in {
    val model = new LinReg()
      .setNIter(1)
      .setLR(2)
      .setLabelCol("target")

    model.getNIter should be(1)
    model.getLR should be(2)
    model.getLabelCol should be("target")
  }

  "Dataframe" should "contains" in {
    data.schema.fieldNames.contains("features") should be(true)
    data.schema.fieldNames.contains("target") should be(true)
  }

  "Model" should "predict target" in {
    val model: LinRegModel =
      new LinRegModel(Vectors.fromBreeze(DenseVector(0, 0, 0, 0)))
        .setOutputCol("prediction")

    val vectors: Array[Vector] =
      model.transform(data).collect().map(_.getAs[Vector](0))

    vectors.length should be(100)
  }

  "Estimator" should "estimate parameters" in {
    val estimator = new LinReg().setOutputCol("predicted").setNIter(5)

    val model = estimator.fit(data)

    model.isInstanceOf[LinRegModel] should be(true)
  }
}

object LinRegTest extends WithSpark {
  val _generator: RandBasis = RandBasis.withSeed(0)

  lazy val _x: DenseMatrix[Double] = DenseMatrix.rand(100, 3, rand = _generator.uniform)

  lazy val _weights: DenseVector[Double] = DenseVector[Double](-1, 0, 1)

  lazy val _y: DenseVector[Double] = (_x * _weights.asDenseMatrix.t).toDenseVector

  lazy val _dataDense: DenseMatrix[Double] = DenseMatrix.horzcat(_x, _y.asDenseMatrix.t)

  lazy val _data: DataFrame = {
    import sqlc.implicits._
    Range(0, _x.rows)
      .map(index => (Vectors.fromBreeze(_x(index, ::).t), _y(index)))
      .toDF("features", "target")
  }

}
