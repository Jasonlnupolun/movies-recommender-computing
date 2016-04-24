package org.miejski.recommendations.recommendation

import org.apache.log4j.Logger
import org.miejski.recommendations.evaluation.model.{User, MovieRating}
import org.miejski.recommendations.model.{Movie, UserRating}
import org.miejski.recommendations.neighbours.{NeighbourInfo, Neighbours, UserAverageRating}
import org.miejski.recommendations.parser.DoubleFormatter

class CFMoviesRecommender(neighbours: Neighbours,
                          users: List[User],
                          moviesRatings: List[(Movie, Seq[UserRating])],
                          predictForMovie: (UserAverageRating, Set[NeighbourInfo], Seq[UserRating]) => Option[Double]) extends Serializable
  with DoubleFormatter
  with MovieRecommender {

  @transient lazy val log = Logger.getLogger(getClass.getName)

  def findRatings(user: String, moviesToPredict: List[MovieRating]): List[MovieRating] = {
    // TODO change to List[MovieId]
    val closestNeighbours: Set[NeighbourInfo] = neighbours.findFor(user, 20).toSet
    if (closestNeighbours.isEmpty) {
      log.info("Closest neighbours are empty")
      return List.empty
    }

    val closestNeighboursIds = closestNeighbours.map(_.neighbourName)
    val moviesIdToPredictRatings = moviesToPredict.map(ratings => ratings.movie.id).toSet

    val userAverageRating = neighbours.getUserAverageRating(user)

    val neighboursMoviesRatings = neighboursRatingsOfSelectedMovies(users, closestNeighboursIds, moviesIdToPredictRatings)

    val neighboursRatingsForGivenMovies: List[(Movie, Seq[UserRating])] = moviesRatings.filter(movieRating => moviesIdToPredictRatings.contains(movieRating._1.id))
      .map(mRating => (mRating._1, mRating._2.filter(userRating => closestNeighboursIds.contains(userRating.user))))

    val predictedRatings = neighboursRatingsForGivenMovies
      .map(nr => (nr._1, predictForMovie(userAverageRating, closestNeighbours, nr._2)))
      .map(rating => MovieRating(rating._1, rating._2))

    if (predictedRatings.toList.size != moviesToPredict.size) {
      println()
    }
    predictedRatings.toList
  }

  def neighboursRatingsOfSelectedMovies(users: List[User], closestNeighboursIds: Set[String], moviesIdToPredictRatings: Set[String]): List[(String, List[MovieRating])] = {
    users.filter(u => closestNeighboursIds.contains(u.id))
      .map(closestNeighbour => (closestNeighbour.id, closestNeighbour.ratings.filter(movieRating => moviesIdToPredictRatings.contains(movieRating.movie.id))))
  }

  def forUser(user: String, top: Int = 0): Seq[(Movie, Double)] = {
    //    val closestNeighbours: Set[NeighbourInfo] = neighbours.findFor(user).toSet
    //    if (closestNeighbours.isEmpty) {
    //      log.info("Closest neighbours are empty")
    //      return Seq.empty
    //    }
    //    val closestNeighboursIds = closestNeighbours.map(_.neighbourName)
    //
    //    val userAverageRating = neighbours.getUserAverageRating(user)
    //
    //    val neighboursRatings = moviesRatings.map(mRating => (mRating._1, mRating._2.filter(userRating => closestNeighboursIds.contains(userRating.user))))
    //    val predictedRatings = neighboursRatings
    //      .map(nr => (nr._1, predictForMovie(userAverageRating, closestNeighbours, nr._2)))
    //
    //    val moviesSortedByPredictedRating = predictedRatings.filter(_._2.isDefined)
    //      .map(s => (s._1, s._2.get))
    //      .sortBy(s => s._2)
    //      .map(prediction => (prediction._1, format(prediction._2)))
    //      .reverse
    //    if (top <= 0) moviesSortedByPredictedRating else moviesSortedByPredictedRating.take(top)
    Seq.empty
  }
}

object CFMoviesRecommender {

  def standardPrediction(user: UserAverageRating, neighbours: Set[NeighbourInfo], neighboursRatings: Seq[UserRating]): Option[Double] = {
    val neighboursSimilarityMap = neighbours.map(n => (n.neighbourName, n.similarity)).toMap
    val ratingWithSimilarity = neighboursRatings.map(r => (r.user, r.rating, neighboursSimilarityMap.getOrElse(r.user, 0.0)))
    val counter = ratingWithSimilarity.map(neighbourRatingDetails => neighbourRatingDetails._2.getOrElse(0.0) * neighbourRatingDetails._3).sum
    val delimeter = ratingWithSimilarity.map(ur => if (ur._2.isEmpty) 0 else ur._3).sum

    if (delimeter == 0) Option.empty else Option(counter / delimeter)
  }

  def averageNormalizedPrediction(user: UserAverageRating, neighbours: Set[NeighbourInfo], neighboursRatings: Seq[UserRating]): Option[Double] = {
    val neighboursSimilarityMap = neighbours.map(n => (n.neighbourName, n)).toMap
    val neighboursSimiliarityMap = neighbours.map(neighbour => (neighbour.neighbourName, neighbour))
      .groupBy(_._1).mapValues(_.map(_._2))
    val ratingWithSimiliarity = neighboursRatings.map(r => (r.user, r.rating, neighboursSimilarityMap.get(r.user).get))
    val counter = ratingWithSimiliarity.filter(_._2.isDefined)
      .map(userRating => (userRating._2.get - userRating._3.neighbourAverageRating) * userRating._3.similarity)
      .sum
    val delimeter = ratingWithSimiliarity.map(ur => if (ur._2.isEmpty) 0 else ur._3.similarity).sum

    if (delimeter == 0) Option.empty else Option(user.averageRating + (counter / delimeter))
  }

}

case class UserRatingWithSimilarity(user: String, rating: Option[Double], similarity: Double)