package org.miejski.recommendations.evaluation

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.model.{Movie, UserRating}
import org.miejski.recommendations.neighbours.Neighbours
import org.miejski.recommendations.recommendation.MovieRecommender

class RecommenderEvaluator extends Serializable {

  def evaluateRecommender(usersRatings: RDD[User],
                          dataSplitter: (RDD[User]) => List[ValidationDataSplit],
                          recommenderCreator: (Neighbours, RDD[(Movie, Seq[UserRating])]) => MovieRecommender) = {
    val validationDataSplit = dataSplitter(usersRatings)
    val foldsErrors = validationDataSplit.map(dataSplit => foldError(dataSplit, recommenderCreator))
    val error = foldsErrors.sum / foldsErrors.length
    println(s"Evaluator final error : $error")
    error
  }

  def foldError(crossValidation: ValidationDataSplit,
                recommenderCreator: (Neighbours, RDD[(Movie, Seq[UserRating])]) => MovieRecommender): Double = {
    val moviesRatings = crossValidation.trainingData.flatMap(s => s.ratings.map(rating => (rating.movie, UserRating(s.id, rating.rating))))
      .groupByKey().map(s => (s._1, s._2.toSeq)) // TODO needed?

    val neighbours: Neighbours = Neighbours.fromUsers(User.createCombinations(crossValidation.trainingData.collect().toSeq)) // TODO RDD mixed with collection
    val recommender = recommenderCreator(neighbours, moviesRatings)

    val usersPredictionsErrors = crossValidation.testData.collect().map(user => calculateRootMeanSquareError(user.ratings, recommender.findRatings(user.id, user.ratings), user.id))

    val singleFoldAvgError: Double = usersPredictionsErrors.sum / usersPredictionsErrors.length
    println(s"Single fold error: $singleFoldAvgError")
    singleFoldAvgError
  }

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