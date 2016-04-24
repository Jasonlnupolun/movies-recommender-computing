package org.miejski.recommendations.evaluation.partitioning

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.{MovieRating, User}

class RandomPartitioner extends Partitioner {
  override def splitData(usersRatings: RDD[User], k: Int = 5, testDataRatio: Double = 0.2): List[ValidationDataSplit] = {
    val allRatings = usersRatings.flatMap(user => user.ratings.map(rating => (user.id, rating)))
    val ratingsCount = allRatings.count()

    val validationSets = for (x <- 0 to k) yield {
      val splitRatings = allRatings.randomSplit(Array(testDataRatio, 1 - testDataRatio))
      val testUsers = groupByUser(splitRatings.head)
      val trainingUsers = groupByUser(splitRatings.tail.head)

      ValidationDataSplit(testUsers, trainingUsers)
    }

    validationSets.toList
  }

  def groupByUser(movieRatings: RDD[(String, MovieRating)]): RDD[User] = {
    movieRatings.groupBy(_._1).map(x => User(x._1, x._2.map(_._2).toList))
  }
}
