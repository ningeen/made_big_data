package org.apache.spark.ml.made

import breeze.linalg.DenseVector
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.{DoubleParam, IntParam, ParamMap}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsReader, DefaultParamsWritable, DefaultParamsWriter, Identifiable, MLReadable, MLReader, MLWritable, MLWriter, SchemaUtils}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import org.apache.spark.sql.types.StructType


trait LinRegParams extends HasFeaturesCol with HasLabelCol with HasOutputCol {

  val lr = new DoubleParam(this, "lr", "Learning rate")

  val nIter = new IntParam(this, "nIter", "Number of iterations")

  def setLR(value: Double): this.type = set(lr, value)

  def setNIter(value: Int): this.type = set(nIter, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def getLR: Double = $(lr)

  def getNIter: Int = $(nIter)

  setDefault(lr -> 3e-4)

  setDefault(nIter -> 100)

  setDefault(featuresCol -> "features")

  setDefault(labelCol -> "target")

  setDefault(outputCol -> "prediction")

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkNumericType(schema, getLabelCol)

    if (schema.fieldNames.contains($(outputCol))) {
      SchemaUtils.checkColumnType(schema, getOutputCol, new VectorUDT())
      schema
    } else {
      SchemaUtils.appendColumn(schema, schema(getFeaturesCol).copy(name = getOutputCol))
    }
  }
}

class LinRegModel private[made](override val uid: String, val coef: Vector)
  extends Model[LinRegModel]
    with LinRegParams
    with MLWritable {

  private[made] def this(weights: Vector) =
    this(Identifiable.randomUID("LinRegModel"), weights)

  override def copy(extra: ParamMap): LinRegModel =
    copyValues(new LinRegModel(coef), extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val coefBreeze = coef.asBreeze.toDenseVector
    val bias = coefBreeze(-1)
    val weights = Vectors.fromBreeze(coefBreeze(0 to coefBreeze.size - 2))

    val transformUdf =
      dataset.sqlContext.udf.register(
        uid + "_transform",
        (x: Vector) => {x.dot(weights) + bias}
      )

    dataset.withColumn($(outputCol), transformUdf(dataset($(featuresCol))))
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def write: MLWriter = new DefaultParamsWriter(this) {
    override protected def saveImpl(path: String): Unit = {
      super.saveImpl(path)
      val coefficients = Seq(Tuple1(coef))
      sqlContext
        .createDataFrame(coefficients)
        .write
        .parquet(path + "/weights")
    }
  }
}

class LinReg(override val uid: String)
  extends Estimator[LinRegModel]
    with LinRegParams
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("LinReg"))

  private def calcGrad(
                        featuresWithTarget: Vector,
                        weights: DenseVector[Double],
                        bias: Double
                      ): (DenseVector[Double], Double) = {
    val breezeVector = featuresWithTarget.asBreeze
    val features = breezeVector(0 until breezeVector.length - 1)
    val target = breezeVector(-1)
    val predict = (features dot weights) + bias
    val error = target - predict
    val gradWeights = -2.0 * features.toDenseVector * error
    val gradBias = -2.0 * error
    Tuple2(gradWeights, gradBias)
  }

  private def optimize(vectors: Dataset[Vector], modelDim: Int) = {
    val totalIterations = getNIter
    val lr = getLR

    val gaussDistribution = breeze.stats.distributions.Gaussian(0, 1)
    val weights = DenseVector.rand[Double](modelDim, gaussDistribution)
    var bias: Double = 0

    for (_ <- 0 to totalIterations) {
      val dataRdd = vectors.rdd
      val weightsRdd = dataRdd.sparkContext.broadcast(weights)

      val (gradWeights, gradBias) = dataRdd
        .map(row => calcGrad(row, weightsRdd.value, bias))
        .reduce((gradLhs, gradRhs) => Tuple2(gradLhs._1 + gradRhs._1, gradLhs._2 + gradRhs._2))

      val step = lr / dataRdd.count()
      weights -= step * gradWeights
      bias -= step * gradBias
    }

    val solution = DenseVector.vertcat(weights, DenseVector(bias))
    solution
  }

  override def fit(dataset: Dataset[_]): LinRegModel = {
    implicit val encoder: Encoder[Vector] = ExpressionEncoder()

    val assembler = new VectorAssembler()
      .setInputCols(Array($(featuresCol), $(labelCol)))
      .setOutputCol($(outputCol))
    val transformed = assembler.transform(dataset)
    val data: Dataset[Vector] = transformed.select(transformed($(outputCol))).as[Vector]

    val modelDim: Int = AttributeGroup
      .fromStructField(dataset.schema($(featuresCol)))
      .numAttributes
      .getOrElse(data.first().size - 1)

    val solution: DenseVector[Double] = optimize(data, modelDim)
    copyValues(new LinRegModel(Vectors.fromBreeze(solution))).setParent(this)
  }

  override def copy(extra: ParamMap): Estimator[LinRegModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

}

object LinReg extends DefaultParamsReadable[LinReg]

object LinRegModel extends MLReadable[LinRegModel] {
  override def read: MLReader[LinRegModel] = new MLReader[LinRegModel] {
    override def load(path: String): LinRegModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc)

      val vectors = sqlContext.read.parquet(path + "/weights")

      implicit val encoder: Encoder[Vector] = ExpressionEncoder()

      val weights = vectors.select(vectors("_1").as[Vector]).first()

      val model = new LinRegModel(weights)
      metadata.getAndSetParams(model)
      model
    }
  }
}
