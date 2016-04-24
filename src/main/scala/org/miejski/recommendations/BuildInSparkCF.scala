package org.miejski.recommendations

import java.io.File

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.evaluation.{CrossValidationPartitioner, RecommenderEvaluator}
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

    new RecommenderEvaluator().evaluateRecommender(users,
      (dataSplitter) => new CrossValidationPartitioner().allCombinationsTimestampBased(dataSplitter),
      (neighbours, ratings) => MatrixFactorizationRecommender(ratings))

    sc.stop()
  }

//  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
//    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
//    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
//      .join(data.map(x => ((x.user, x.product), x.rating)))
//      .values
//    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
//  }
}
