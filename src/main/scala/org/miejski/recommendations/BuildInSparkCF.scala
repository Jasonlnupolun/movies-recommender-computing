package org.miejski.recommendations

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.miejski.recommendations.evaluation.CrossValidationPartitioner
import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.model.Movie

object BuildInSparkCF {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("MovieLensBuiltInCF")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "6g")
      .set("ALS.checkpointingInterval", "2")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("checkpoint/")
    val ratings = sc.textFile(new File("src/main/resources/u.data_no_header").toString).map { line =>
      val fields = line.split(",")
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val users = ratings.map(rating => (rating._2.user, rating._2, rating._1))
      .groupBy(s => s._1)
      .map(gR => User(gR._1.toString, gR._2.map(x => MovieRating(Movie(x._2.product.toString, x._2.product.toString), Option.apply(x._2.rating), x._3)).toList))

    val validationDataSplits = new CrossValidationPartitioner().allCombinationsTimestampBased(users)
    val sparkDatasets = validationDataSplits.map(vds => (vds.trainingData.flatMap(_.toMLibRatings()), vds.testData.flatMap(_.toMLibRatings())))

    val movies = sc.textFile(new File("src/main/resources/u.item").toString).map { line =>
      val fields = line.split("\\|")
      (fields(0).toInt, fields(1))
    }.collect().toMap

    val training = sparkDatasets.apply(0)._1
    val test = sparkDatasets.apply(0)._1

    val numTest = test.count()
    val numTraining = training.count()

    val ranks = List(12)
    val lambdas = List(0.01)
    val numIters = List(30)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, test, numTest)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    val testRmse = computeRmse(bestModel.get, test, numTest)

    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    sc.stop()
  }


  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

}
