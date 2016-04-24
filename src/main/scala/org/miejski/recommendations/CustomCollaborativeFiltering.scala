package org.miejski.recommendations

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.miejski.recommendations.evaluation.model.User
import org.miejski.recommendations.evaluation.partitioning.CrossValidationPartitioner
import org.miejski.recommendations.model.UserRating
import org.miejski.recommendations.parser.movielens.MovielensRatingsParser
import org.miejski.recommendations.recommendation.CFMoviesRecommender

object CustomCollaborativeFiltering {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("MovieLensBuiltInCF")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "6g")
      .set("ALS.checkpointingInterval", "2")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("checkpoint/")

    val sqlContext = new SQLContext(sc)
    sqlContext.getAllConfs.foreach(println)

    val ratingsDataframe = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("src/main/resources/u.data")

    val allRatings = ratingsDataframe.rdd.map(MovielensRatingsParser.parseRating).cache()
    val usersGroupedRatings = allRatings.groupByKey().map(User.fromTuple)
    val toSeq: Seq[User] = usersGroupedRatings.collect().toSeq

    val moviesRatings = allRatings.map(r => (r._2.movie, UserRating(r._1, r._2.rating)))
      .groupByKey()
      .map(s => (s._1, s._2.toSeq)).cache()

    val collectedMoviesRatings = moviesRatings.collect().toList

    new ModelEvaluatorRunner().runSimulation(
      sc,
      new CrossValidationPartitioner,
      (neighbours, mRatings) => new CFMoviesRecommender(neighbours, toSeq.toList, collectedMoviesRatings, CFMoviesRecommender.averageNormalizedPrediction))

    sc.stop()
  }
}
