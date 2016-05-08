package org.miejski.recommendations.recommendation

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.MovieRating
import org.miejski.recommendations.model.{Movie, UserRating}

class AverageRatingRecommender(moviesAverageRating: Map[String, Double]) extends MovieRecommender {
  override def findRatings(user: String, moviesToPredict: List[MovieRating]): List[MovieRating] = {
    moviesToPredict.map(m => MovieRating(m.movie, moviesAverageRating.get(m.movie.id)))
  }

  override def forUser(user: String, top: Int): Seq[(Movie, Double)] = ???
}

object AverageRatingRecommender {

  def apply(ratings: RDD[(Movie, Seq[UserRating])]): AverageRatingRecommender = {
    val averageMovieRatings = ratings.map(movieRatings => (movieRatings._1.id, averageRating(movieRatings._2))).collect().toMap
    new AverageRatingRecommender(averageMovieRatings)
  }


  def averageRating(ratings: Seq[UserRating]): Double = {
    val extractedRatings = ratings.map(_.rating)
      .filter(_.nonEmpty)
      .map(_.get)

    extractedRatings.foldLeft(0.0)(_ + _) / extractedRatings.size.toDouble
  }
}
