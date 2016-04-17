package org.miejski.recommendations.evaluation.model

import org.miejski.recommendations.model.Movie

case class MovieRating(movie: Movie, rating: Option[Double], timestamp: Long = -1)

case class User(id: String, ratings: List[MovieRating]) extends Serializable

object User {

  def fromTuple(tuple: Tuple2[String, Iterable[MovieRating]]) = {
    User(tuple._1, tuple._2.toList)
  }


  def createCombinations(users: Seq[User]) = {
    users.combinations(2).map(x => (x.head, x.tail.head)).toSeq
  }
}