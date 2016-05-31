package org.miejski.recommendations.evaluation

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.error.{RMSE, RootMeanSquareErrorMetric}
import org.miejski.recommendations.evaluation.model.User
import org.miejski.recommendations.evaluation.partitioning.{Partitioner, ValidationDataSplit}
import org.miejski.recommendations.evaluation.rank.MeanReciprocalRankMetric
import org.miejski.recommendations.model.{Movie, UserRating}
import org.miejski.recommendations.neighbours.Neighbours
import org.miejski.recommendations.recommendation.MovieRecommender

class RecommenderEvaluator(metrics: List[RecommenderMetric] = List()) extends Serializable {

  def evaluateRecommender(usersRatings: RDD[User],
                          dataSplitter: Partitioner,
                          recommenderCreator: (Neighbours, RDD[(Movie, Seq[UserRating])]) => MovieRecommender) = {
    val validationDataSplit = dataSplitter.splitData(usersRatings)
    validationDataSplit.foreach(dataSplit => foldError(dataSplit, recommenderCreator))
    metrics.foreach(_.printResult())
  }

  def foldError(validationFold: ValidationDataSplit,
                recommenderCreator: (Neighbours, RDD[(Movie, Seq[UserRating])]) => MovieRecommender): Unit = {
    val recommender: MovieRecommender = createRecommender(validationFold, recommenderCreator)
    metrics.foreach(_.updateMetrics(recommender, validationFold))
  }

  def createRecommender(crossValidation: ValidationDataSplit, recommenderCreator: (Neighbours, RDD[(Movie, Seq[UserRating])]) => MovieRecommender): MovieRecommender = {
    val moviesRatings = crossValidation.trainingData.flatMap(s => s.ratings.map(rating => (rating.movie, UserRating(s.id, rating.rating))))
      .groupByKey().map(s => (s._1, s._2.toSeq)) // TODO needed?

    val neighbours: Neighbours = Neighbours.fromUsers(User.createCombinations(crossValidation.trainingData.collect().toSeq)) // TODO RDD mixed with collection
    recommenderCreator(neighbours, moviesRatings)
  }
}

object RecommenderEvaluator {
  def apply() = {
    val metrics = List(new RootMeanSquareErrorMetric(RMSE.calculateRootMeanSquareError), new MeanReciprocalRankMetric)
    new RecommenderEvaluator(metrics)
  }
}
