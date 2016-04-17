package org.miejski.recommendations.correlation

import org.miejski.recommendations.evaluation.model.MovieRating
import org.miejski.recommendations.parser.DoubleFormatter

import scala.math.{pow, sqrt}

class PearsonCorrelation {


}

object PearsonCorrelation extends DoubleFormatter {

  def compute(user1Ratings: List[MovieRating],
              user2Ratings: List[MovieRating],
              weighting: (Double, Int, Int) => Double = significanceWeighting): NeighboursDetails = {
    val sharedRatings = (user1Ratings.map(s => (s.movie.id, s.rating)) ++ user2Ratings.map(s => (s.movie.id, s.rating)))
      .groupBy(_._1)
      .filter { case kv => kv._2.length == 2 }
      .values
      .map(s => (s.apply(0)._2.get, s.apply(1)._2.get)) // TODO add userId to make sure its ok
      .toSeq

    val user1SharedRatings = sharedRatings.map(_._1)
    val user2SharedRatings = sharedRatings.map(_._2)

    val user1SharedMean = mean(user1SharedRatings)
    val user2SharedMean = mean(user2SharedRatings)

    val user1TotalMean = mean(user1Ratings.map(s => s.rating.get))
    val user2TotalMean = mean(user2Ratings.map(s => s.rating.get))

    if (sharedRatings.isEmpty) return NeighboursDetails(-1, format(user1SharedMean), format(user2SharedMean), user1TotalMean, user2TotalMean)

    val counter = sharedRatings
      .map(s => (s._1 - user1SharedMean) * (s._2 - user2SharedMean)).sum

    val correlation = calculateCorrelation(user1SharedRatings,
      user2SharedRatings,
      user1SharedMean,
      user2SharedMean,
      counter,
      weighting)
    val details: NeighboursDetails = NeighboursDetails(format(correlation), format(user1SharedMean), format(user2SharedMean), user1TotalMean, user2TotalMean)
    details
  }

  def calculateCorrelation(user1SharedRatings: Seq[Double],
                           user2SharedRatings: Seq[Double],
                           user1SharedMean: Double,
                           user2SharedMean: Double,
                           counter: Double,
                           weighting: (Double, Int, Int) => Double): Double = {
    val correlation = if (counter == 0.0) 1.0 else counter / (singleUserDenominator(user1SharedRatings, user1SharedMean) * singleUserDenominator(user2SharedRatings, user2SharedMean))
    weighting(correlation, user1SharedRatings.size, 10)
  }

  def significanceWeighting(correlation: Double, sharedMoviesCount: Int, threshold: Int = 10): Double = {
    correlation * sharedMoviesCount / math.max(sharedMoviesCount, threshold)
  }

  def singleUserDenominator(userRatings: Seq[Double], mean: Double) = {
    sqrt(userRatings.map(s => pow(s - mean, 2)).sum)
  }

  private def mean(ratings: Seq[Double]): Double = {
    val result = ratings.sum / ratings.length
    if (result.equals(Double.NaN)) 0.0 else result
  }
}

case class NeighboursDetails(similarity: Double,
                             user1SharedAverageRating: Double,
                             user2SharedAverageRating: Double,
                             user1AverageRating: Double,
                             user2AverageRating: Double)

