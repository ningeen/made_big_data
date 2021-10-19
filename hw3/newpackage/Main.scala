package newpackage

import scala.util.Random
import io.Source
import breeze.linalg.{DenseMatrix, _}
import breeze.stats._

import java.io.File
import scala.collection.mutable.ListBuffer


object Main {
  def getFeatures(path: String): (DenseMatrix[Double], DenseVector[Double]) = {
    val file = Source.fromFile(path)
    val lines = file.getLines.drop(1).toVector.map(_.split(','))

    var featuresVert = DenseMatrix(for (row <- lines) yield row(4).toInt)
    for (index <- 5 to 11) {
      val numericFeat = DenseMatrix(for (row <- lines) yield row(index).toInt)
      featuresVert = DenseMatrix.vertcat(featuresVert, numericFeat)
    }

    val firstTypeUnique = (for (row <- lines) yield row(2)).distinct
    val secondTypeUnique = (for (row <- lines) yield row(3)).distinct
    val typeUnique = (firstTypeUnique ++ secondTypeUnique).distinct.filter(_ != "")
    for (index <- 1 to typeUnique.size) {
      val numericFeat = DenseMatrix(
        for (row <- lines)
          yield if (row(2) == typeUnique(index - 1) | row(3) == typeUnique(index - 1)) 1 else 0
      )
      featuresVert = DenseMatrix.vertcat(featuresVert, numericFeat)
    }

    val legendary = DenseMatrix(for (row <- lines) yield if (row(12) == "True") 1.0 else 0.0).toDenseVector

    val featuresVertDouble = convert(featuresVert.t, Double)

    println("Features loaded")
    (featuresVertDouble, legendary)
  }

  def normalizeData(
    trainSet: DenseMatrix[Double],
    valSet: DenseMatrix[Double]
  ): (DenseMatrix[Double], DenseMatrix[Double]) = {
    val valuesMean = mean(trainSet(::, *)).t.asDenseMatrix
    val valuesStd = stddev(trainSet(::, *)).t.asDenseMatrix

    val trainSetDoubleNorm = (trainSet - tile(valuesMean, trainSet.rows)) / tile(valuesStd, trainSet.rows)
    val valSetDoubleNorm = (valSet - tile(valuesMean, valSet.rows)) / tile(valuesStd, valSet.rows)
    (trainSetDoubleNorm, valSetDoubleNorm)
  }

  def calcCvScore(featuresVert: DenseMatrix[Double], legendary: DenseVector[Double]): Double = {
    val numSplits = 5
    val length = featuresVert.rows - 1
    val indices = Random.shuffle((0 to length).toVector)

    val splitLen = featuresVert.rows / numSplits

    val scores = ListBuffer[Double]()
    for (nSplit <- 1 to numSplits) {
      val valIndices = indices slice((nSplit - 1) * splitLen, nSplit * splitLen)
      val trainIndices = (indices slice(0, (nSplit - 1) * splitLen)) ++ (indices slice(nSplit * splitLen, featuresVert.rows))

      val valSet = featuresVert(valIndices, ::).toDenseMatrix
      val trainSet = featuresVert(trainIndices, ::).toDenseMatrix
      val valTarget = legendary(valIndices).toDenseVector
      val trainTarget = legendary(trainIndices).toDenseVector

      val (trainSetNorm, valSetNorm) = normalizeData(trainSet, valSet)

      val model = new LogReg(trainSet.cols + 1)
      model.fit(trainSetNorm, trainTarget)
      val valPrediction = model.predict(valSetNorm)

      val score = mean((valTarget :== valPrediction).mapValues(if (_) 1.0 else 0.0))
      println(s"Iteration #$nSplit accuracy score: $score")
      scores.append(score)
    }

    mean(scores)
  }

  def trainTestSplit(
    x: DenseMatrix[Double],
    y: DenseVector[Double],
    trainSize: Int
  ): (DenseMatrix[Double], DenseVector[Double], DenseMatrix[Double], DenseVector[Double]) = {
    val length = x.rows - 1
    val indices = Random.shuffle((0 to length).toVector)

    val trainIndices = indices slice(0, trainSize)
    val testIndices = indices slice(trainSize, x.rows)

    val testSet = x(testIndices, ::).toDenseMatrix
    val trainSet = x(trainIndices, ::).toDenseMatrix
    val testTarget = y(testIndices).toDenseVector
    val trainTarget = y(trainIndices).toDenseVector
    println(s"Data has been split. Train size: ${trainSet.rows}. Test size: ${testSet.rows}")
    (trainSet, trainTarget, testSet, testTarget)
  }

  def getPrediction(
    trainSet: DenseMatrix[Double],
    trainTarget: DenseVector[Double],
    testSet: DenseMatrix[Double],
    testTarget: DenseVector[Double]
  ): DenseVector[Double] = {
    val (trainSetNorm, testSetNorm) = normalizeData(trainSet, testSet)
    val model = new LogReg(trainSet.cols + 1)
    model.fit(trainSetNorm, trainTarget)
    val trainPrediction = model.predict(trainSetNorm)
    val testPrediction = model.predict(testSetNorm)

    val trainScore = mean((trainTarget :== trainPrediction).mapValues(if (_) 1.0 else 0.0))
    val testScore = mean((testTarget :== testPrediction).mapValues(if (_) 1.0 else 0.0))

    println(s"Train accuracy score: $trainScore")
    println(s"Test accuracy score: $testScore")
    testPrediction
  }

  def main(args: Array[String]): Unit = {
    val path = args(0)
    val pathOutput = args(1)

    val (featuresVert, legendary) = getFeatures(path)

    val (trainSet, trainTarget, testSet, testTarget) = trainTestSplit(featuresVert, legendary, trainSize = 600)

    val cvScore = calcCvScore(trainSet, trainTarget)
    println(s"CV accuracy score: $cvScore")

    val testPrediction = getPrediction(trainSet, trainTarget, testSet, testTarget)

    csvwrite(new File(pathOutput), testPrediction.toDenseMatrix.t, separator=',')
    println(s"Prediction saved to $pathOutput")
  }
}
