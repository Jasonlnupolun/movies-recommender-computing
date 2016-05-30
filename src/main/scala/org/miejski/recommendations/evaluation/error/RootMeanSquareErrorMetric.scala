package org.miejski.recommendations.evaluation.error

import org.miejski.recommendations.evaluation.RecommenderMetric
import org.miejski.recommendations.evaluation.model.MovieRating
import org.miejski.recommendations.evaluation.partitioning.ValidationDataSplit
import org.miejski.recommendations.recommendation.MovieRecommender

class RootMeanSquareErrorMetric(errorMethod: (List[MovieRating], List[MovieRating], String) => Double) extends RecommenderMetric {


  var foldsCount = 0
  var foldsErrors: List[Double] = List()

  override def updateMetrics(recommender: MovieRecommender, validationFold: ValidationDataSplit): Unit = {
    val usersPredictionsErrors = validationFold.testData
      .collect()
      .map(user => errorMethod(user.ratings, recommender.findRatings(user.id, user.ratings), user.id))

    val singleFoldAvgError: Double = usersPredictionsErrors.sum / usersPredictionsErrors.length
    foldsErrors = foldsErrors :+ singleFoldAvgError
    foldsCount += 1
  }

  override def printResult(): Unit = {
    val error = calculateError()
    println(s"Evaluator final error : $error")
  }

  def calculateError() = foldsErrors.sum / foldsErrors.length
}
