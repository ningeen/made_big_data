package hw5

import breeze.linalg._
import org.apache.spark.sql._
import org.apache.spark.ml._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.made.{LinReg, LinRegModel}

object main {
  def getFeatures: DenseMatrix[Double] = {
    val X = DenseMatrix.rand[Double](100000, 3)
    val weights = DenseVector(1.5, 0.3, -0.7)
    val y = X * weights
    val data = DenseMatrix.horzcat(X, y.asDenseMatrix.t)

    println("Features loaded")
    println("Weights:", weights)
    data
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("hw5")
      .getOrCreate()

    import spark.implicits._
    val data = getFeatures
    val df = data(*, ::).iterator
      .map(x => (x(0), x(1), x(2), x(3)))
      .toSeq.toDF("x1", "x2", "x3", "y")

    println(df.show(1))

    val pipeline = new Pipeline().setStages(
      Array(
        new VectorAssembler()
          .setInputCols(Array("x1", "x2", "x3"))
          .setOutputCol("features"),
        new LinReg()
          .setLabelCol("y")
          .setLR(3e-1)
          .setNIter(500)
          .setOutputCol("prediction")
      )
    )

    println("Fitting model")
    val model = pipeline.fit(df)
    val w = model.stages.last.asInstanceOf[LinRegModel].coef

    println("Transform df")
    val dfNew = model.transform(df)

    println(dfNew.show(1))
    println("Model weights", w)
  }
}