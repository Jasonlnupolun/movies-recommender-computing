package org.miejski.recommendations.parser.movielens

import org.apache.spark.sql.Row
import org.miejski.recommendations.evaluation.model.MovieRating
import org.miejski.recommendations.model.Movie

object MovielensRatingsParser {

  def parseRating(row: Row): (String, MovieRating) = {
    val values = row.toSeq
    val userId: String = values.head.toString
    val movieId: String = values.apply(1).toString
    val rating: Long = values.apply(2).toString.toLong
    val timestamp : Long = values.apply(3).toString.toLong
    (userId, MovieRating(Movie.id(movieId), Option.apply(rating), timestamp) )
  }

}
