package org.miejski.recommendations.evaluation.error

import org.miejski.recommendations.evaluation.model.MovieRating

object RMSE {

  def calculateRootMeanSquareError(originalRatings: List[MovieRating], predictedRatings: List[MovieRating], user: String): Double = {
    assert(predictedRatings.length == originalRatings.length)
    val ratingsGroupedByMovie: Map[String, List[MovieRating]] = (originalRatings ++ predictedRatings).groupBy(_.movie.id)
    val filter: Map[String, List[MovieRating]] = ratingsGroupedByMovie
      .filter(_._2.count(_.rating.nonEmpty) == 2)
    val squaredRatingsDiff = filter
      .map(kv => kv._2.map(_.rating.get).reduce((a, b) => Math.pow(a - b, 2.0)))

    val singleUserError: Double = math.sqrt(squaredRatingsDiff.sum / predictedRatings.length)
    println(s"$user error: $singleUserError")
    singleUserError
  }
}
