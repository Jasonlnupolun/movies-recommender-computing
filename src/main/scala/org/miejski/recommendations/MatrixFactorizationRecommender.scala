package org.miejski.recommendations

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.MovieRating
import org.miejski.recommendations.model.{Movie, UserRating}
import org.miejski.recommendations.recommendation.MovieRecommender

class MatrixFactorizationRecommender(model: MatrixFactorizationModel, sc: SparkContext) extends MovieRecommender {

  override def findRatings(user: String, moviesToPredict: List[MovieRating]): List[MovieRating] = {
    val userProductTuples: List[(Int, Int)] = moviesToPredict.map(m => (user.toInt, m.movie.id.toInt))
    var predictedRatings: List[MovieRating] = model.predict(sc.parallelize(userProductTuples)).map(r => MovieRating(Movie.id(r.product.toString), Option.apply(r.rating))).collect().toList
    if (userProductTuples.size != predictedRatings.size) {
      val missingMovieRecommendation = userProductTuples.map(_._2.toString).toSet.diff(predictedRatings.map(_.movie.id).toSet)
      predictedRatings = predictedRatings ++ missingMovieRecommendation.map(s => MovieRating(Movie.id(s), Option.empty))
      println()
    }
    predictedRatings
  }

  override def forUser(user: String, top: Int): Seq[(Movie, Double)] = {
    Seq.empty // TODO
  }
}

object MatrixFactorizationRecommender {

  def apply(movieRatings: RDD[(Movie, Seq[UserRating])]) = {
    val sc = movieRatings.sparkContext
    val numTraining = movieRatings.count()

    val ratings = movieRatings.flatMap(x => x._2.map(y => Rating(y.user.toInt, x._1.id.toInt, y.rating.getOrElse(0.0))))
    val rank = 12
    val lambda = 0.01
    val numIter = 30
    val model = ALS.train(ratings, rank, numIter, lambda)
    new MatrixFactorizationRecommender(model, sc)
  }
}