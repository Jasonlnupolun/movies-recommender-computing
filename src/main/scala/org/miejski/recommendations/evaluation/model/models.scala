package org.miejski.recommendations.evaluation.model

import org.apache.spark.mllib.recommendation.Rating
import org.miejski.recommendations.model.Movie

case class MovieRating(movie: Movie, rating: Option[Double], timestamp: Long = -1)

case class User(id: String, ratings: List[MovieRating]) extends Serializable {

  def withTimeOrderedRatings(): User = {
    new User(id, ratings.sortBy(_.timestamp))
  }

  def toMLibRatings() = {
    ratings.map(x => Rating(id.toInt, x.movie.id.toInt, x.rating.getOrElse(0)))
  }

}

object User {

  def fromTuple(tuple: Tuple2[String, Iterable[MovieRating]]) = {
    User(tuple._1, tuple._2.toList)
  }


  def createCombinations(users: Seq[User]) = {
    users.combinations(2).map(x => (x.head, x.tail.head)).toSeq
  }
}