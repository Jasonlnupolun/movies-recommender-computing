package org.miejski.recommendations.evaluation

import java.time.{Instant, LocalDateTime, ZoneId}

import org.miejski.recommendations.SparkSuite
import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.model.Movie

import scala.collection.immutable.IndexedSeq
import scala.util.Random


class CrossValidationPartitionerTest extends SparkSuite {

  def generateRatings(movies: IndexedSeq[Movie]): List[MovieRating] = movies
    .map(m => MovieRating(m, Option.apply(Random.nextDouble()), generateTimestamp()))
    .toList

  def generateTimestamp(): Long = {
    val now: Long = System.currentTimeMillis()
    val nowDate = LocalDateTime.from(Instant.ofEpochMilli(now).atZone(ZoneId.systemDefault()))
    val from = nowDate.minusYears(1).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
    from + (new Random().nextDouble() * (now - from)).toLong
  }

  test("partitioning data equally") {
    val k = 5
    val movies = (100 to 105).map(_.toString).map(movie => new Movie(movie, movie))
    val usersRatings = (1 to 1000).map(id => new User(id.toString, generateRatings(movies)))
    val usersRatingsRdd = sc.parallelize(usersRatings)

    val partitionings = new CrossValidationPartitioner().allCombinations(usersRatingsRdd, k)

    partitionings should have length k
    partitionings.foreach(p => p.trainingData should have length k - 1)
  }
}
