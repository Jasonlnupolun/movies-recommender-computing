package org.miejski.recommendations.evaluation

import java.time.{Instant, LocalDateTime, ZoneId}

import org.miejski.recommendations.SparkSuite
import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.evaluation.partitioning.CrossValidationPartitioner
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
  }


  test("should split users into proper folds") {
    val k: Int = 5
    val movies = (100 to 105).map(_.toString).map(movie => new Movie(movie, movie))
    val usersRatings = (1 to 1000).map(id => new User(id.toString, generateRatings(movies)))

    val usersPartitions = new CrossValidationPartitioner().splitUsersIntoFolds(usersRatings, k)
    usersPartitions should have length k
    usersPartitions.map(_.size) shouldEqual List(200, 200, 200, 200, 200)
  }

  test("should split users ratings by time") {
    val k: Int = 5
    val movies = (101 to 110).map(_.toString).map(movie => new Movie(movie, movie))
    val splitRatio = 0.8
    val moviesCount = movies.size
    val usersRatings = (1 to 1000).map(id => new User(id.toString, generateRatings(movies))).toList
    val usersCount = usersRatings.size

    val (train, test) = new CrossValidationPartitioner().splitByTime(usersRatings, splitRatio)

    train.size shouldEqual usersCount
    test.size shouldEqual usersCount

    train.map(_.ratings.size) shouldEqual List.fill(usersCount)(moviesCount * splitRatio)
    test.map(_.ratings.size) shouldEqual List.fill(usersCount)((moviesCount * (1 - splitRatio)).round)
  }

}
