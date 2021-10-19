package newpackage
import breeze.linalg._
import breeze.numerics._
import breeze.optimize._
import breeze.stats.mean

class LogReg(numFeatures: Int) {
  var weights: DenseVector[Double] = DenseVector.zeros[Double](numFeatures)

  def lrParams(
    x: DenseMatrix[Double],
    y: DenseVector[Double],
    coefficients: DenseVector[Double]
  ): (Double, DenseVector[Double]) = {

    val hypo = sigmoid(x * coefficients)
    val pos_case = y * log(hypo)
    val neg_case = (-y + 1d) * log(-hypo + 1d)
    val cost = -mean(pos_case + neg_case)

    val error = hypo - y
    val grad = x.t * error

    (cost, grad)
  }

  def predict_probability(x: DenseMatrix[Double]): DenseVector[Double] = {
    val x_ones = DenseMatrix.horzcat(x, DenseMatrix.ones[Double](x.rows, 1))
    val logits = x_ones * weights
    sigmoid(logits)
  }

  def predict(x: DenseMatrix[Double]): DenseVector[Double] = {
    val probabilities = predict_probability(x)
    (probabilities >:> 0.5).mapValues(if (_) 1.0 else 0.0)
  }

  def fit(x: DenseMatrix[Double], y: DenseVector[Double]): Unit = {
    val x_ones = DenseMatrix.horzcat(x, DenseMatrix.ones[Double](x.rows, 1))

    val f = new DiffFunction[DenseVector[Double]] {
      def calculate(parameters:DenseVector[Double]): (Double, DenseVector[Double]) = {
        lrParams(x_ones, y, parameters)
      }
    }
    weights = minimize(f, DenseVector.zeros[Double](numFeatures))
  }
}
