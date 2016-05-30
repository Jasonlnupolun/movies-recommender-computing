package org.miejski.recommendations.evaluation

import org.miejski.recommendations.evaluation.partitioning.ValidationDataSplit
import org.miejski.recommendations.recommendation.MovieRecommender

trait RecommenderMetric {
  def updateMetrics(recommender: MovieRecommender, validationFold: ValidationDataSplit): Unit

  def printResult(): Unit
}
