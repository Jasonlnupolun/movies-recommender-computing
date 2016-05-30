package org.miejski.recommendations.evaluation.rank

import org.miejski.recommendations.evaluation.RecommenderMetric
import org.miejski.recommendations.evaluation.model.MovieRating
import org.miejski.recommendations.evaluation.partitioning.ValidationDataSplit
import org.miejski.recommendations.recommendation.MovieRecommender

import scala.annotation.tailrec

class MeanReciprocalRankMetric extends RecommenderMetric {

  var rankForUser: List[(String, Double)] = List()


  override def updateMetrics(recommender: MovieRecommender, validationFold: ValidationDataSplit): Unit = {

    val userRecommendations = validationFold.testData.map(td => (td.id, recommender.forUser(td.id, 100)))
      .mapValues(ratings => ratings.map(singleRating => MovieRating(singleRating._1, Option.apply(singleRating._2))).toList)
      .collect()
    val realRatings = validationFold.testData.map(user => (user.id, user.ratings)).collectAsMap()

    userRecommendations.foreach(userRecommendation => {
      val userTestRatings = realRatings.getOrElse(userRecommendation._1, List())
      val userMeanRaiting: Double = validationFold.trainingData
        .filter(_.id.equals(userRecommendation._1))
        .map(_.meanRating())
        .take(1).head
      val rank: Double = getRecommendationRank(userRecommendation._2.zipWithIndex, userTestRatings, userMeanRaiting)
      rankForUser = rankForUser ++ List((userRecommendation._1, rank))
    })
  }

  override def printResult(): Unit = {

  }


  def getRecommendationRank(recommendations: List[(MovieRating, Int)], userTestRatings: List[MovieRating], userMeanRating: Double): Double = {
    val moviesRatedByUser = userTestRatings.map(_.movie.id)

    def userWouldLikeRecommendedMovie(recs: (MovieRating, Int)): Boolean = {
      moviesRatedByUser.contains(recs._1.movie.id) && recs._1.rating.getOrElse(0.0) > userMeanRating
    }

    @tailrec
    def getRank(recommendations: List[(MovieRating, Int)]): Double = recommendations match {
      case Nil => 0.0
      case head :: tail =>
        if (userWouldLikeRecommendedMovie(head)) 1.0 / head._2
        else getRank(recommendations.tail)
    }

    getRank(recommendations)
  }
}