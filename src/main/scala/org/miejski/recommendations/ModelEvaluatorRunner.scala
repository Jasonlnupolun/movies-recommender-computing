package org.miejski.recommendations

import java.io.File
import java.time.{Duration, LocalDateTime}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.RecommenderEvaluator
import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.evaluation.partitioning.Partitioner
import org.miejski.recommendations.model.{Movie, UserRating}
import org.miejski.recommendations.neighbours.Neighbours
import org.miejski.recommendations.recommendation.MovieRecommender

class ModelEvaluatorRunner {

  def runSimulation(sc: SparkContext,
                    dataSplitter: Partitioner,
                    recommenderCreator: (Neighbours, RDD[(Movie, Seq[UserRating])]) => MovieRecommender) = {

    val users: RDD[User] = readUsersRatings(sc)

    val start = LocalDateTime.now()
    val error = RecommenderEvaluator().evaluateRecommender(users, dataSplitter, recommenderCreator)

    println(s"Final error : $error")
    val end = LocalDateTime.now()

    println(Duration.between(start, end))

    println("Finished")
  }

  def readUsersRatings(sc: SparkContext): RDD[User] = {
    val ratings = sc.textFile(new File("src/main/resources/u.data_no_header").toString).map { line =>
      val fields = line.split(",")
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val users = ratings.map(rating => (rating._2.user, rating._2, rating._1))
      .groupBy(s => s._1)
      .map(gR => User(gR._1.toString, gR._2.map(x => MovieRating(Movie(x._2.product.toString, x._2.product.toString), Option.apply(x._2.rating), x._3)).toList))
    users
  }
}