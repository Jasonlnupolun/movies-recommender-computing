package org.miejski.recommendations.evaluation.error

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.SparkSuite
import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.evaluation.partitioning.ValidationDataSplit
import org.miejski.recommendations.model.Movie
import org.miejski.recommendations.recommendation.MovieRecommender
import org.scalamock.scalatest.MockFactory

class RootMeanSquareErrorMetricTest extends SparkSuite
  with MockFactory {

  test("should calculate error based on what errors from each fold is returned") {
    // given first fold
    var recommender = stub[MovieRecommender]
    (recommender.findRatings _).when("1", *)
      .returns(List(MovieRating(Movie.id("m3"), Option.apply(2.0)), MovieRating(Movie.id("m4"), Option.apply(2.0))))
    var rmseMetric = new RootMeanSquareErrorMetric(errorMethod)

    val trainingData: RDD[User] = sc.parallelize(List())
    val testData = sc.parallelize(List(new User("1", List(mr("m3", 3.0), mr("m4", 4.0)))))
    val dataSplit = new ValidationDataSplit(testData, trainingData)

    //when first update occurs
    rmseMetric.updateMetrics(recommender, dataSplit)

    // given second fold
    recommender = stub[MovieRecommender]
    (recommender.findRatings _).when("1", *)
      .returns(List(MovieRating(Movie.id("m3"), Option.apply(1.0)), MovieRating(Movie.id("m4"), Option.apply(1.0))))

    //when second update occurs
    rmseMetric.updateMetrics(recommender, dataSplit)

    //then
    rmseMetric.foldsCount shouldBe 2
    rmseMetric.foldsErrors should contain allOf(1.5, 2.5)
    rmseMetric.calculateError() shouldBe 2.0
  }

  def errorMethod(m1: List[MovieRating], m2: List[MovieRating], id: String): Double = {
    val ratingsGroupedByMovie = (m1 ++ m2).groupBy(_.movie.id)

    val filter: Map[String, List[MovieRating]] = ratingsGroupedByMovie
      .filter(_._2.count(_.rating.nonEmpty) == 2)

    val ratingsDiff = filter.mapValues(ratings => Math.abs(ratings.head.rating.get - ratings.tail.head.rating.get)).values.toList

    ratingsDiff.sum / ratingsDiff.size.toDouble
  }
}
