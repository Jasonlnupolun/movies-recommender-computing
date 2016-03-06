package org.miejski.recommendations.model

case class UserRating(user: String, rating: Option[Double])

case class Movie(id: String, title: String)

case class MovieId(id: String)

object Movie {
  def apply(movieWithId: String): Movie = {
    val delimeterIndex = movieWithId.indexOf(":")
    Movie(movieWithId.substring(0, delimeterIndex), movieWithId.substring(delimeterIndex + 1).trim)
  }
  def id(id: String) = Movie(id, "")

}

case class MoviePredictedRating(movie: Movie, rating: Double)