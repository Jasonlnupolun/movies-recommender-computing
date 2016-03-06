package org.miejski.recommendations.parser.movielens

import org.apache.spark.sql.Row
import org.miejski.recommendations.evaluation.model.MovieRating
import org.miejski.recommendations.model.Movie
import org.scalatest.{Matchers, FunSuite}

class MovielensRatingsParserTest extends FunSuite
  with Matchers {

  test("Parsing movielens parser") {
    val row: Row = Row("196", "242", "3", "881250949")

    val userRating = MovielensRatingsParser.parseRating(row)

    userRating shouldEqual ("196", MovieRating(Movie.id("242"), Option.apply(3L), 881250949L) )
  }
}
