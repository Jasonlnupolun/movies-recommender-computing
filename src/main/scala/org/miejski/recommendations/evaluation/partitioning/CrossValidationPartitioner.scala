package org.miejski.recommendations.evaluation.partitioning

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.{MovieRating, User}

class CrossValidationPartitioner extends Partitioner {

  def allCombinations(usersRatings: RDD[User], k: Int = 5): List[ValidationDataSplit] = {
    val users = usersRatings.collect().toList

    val usersCount = users.size / 5

    val weights = List.fill(k)(1.0 / k)
    val partitions = usersRatings.randomSplit(weights.toArray, 1).toList

    val indexedPartitions = partitions.zipWithIndex
    indexedPartitions.map(iP => {
      ValidationDataSplit(iP._1, indexedPartitions.filter(p => !p._2.equals(iP._2)).map(_._1).reduce(_ union _))
    })
  }

  def splitByTime(testUsersSet: List[User], trainingProportion: Double = 0.5): (List[User], List[User]) = {
    def getRatingsSplitPosition(ratings: List[MovieRating]): Int = {
      (trainingProportion * ratings.size).toInt
    }
    testUsersSet.map(_.withTimeOrderedRatings())
      .map(user => (user.id, user.ratings.splitAt(getRatingsSplitPosition(user.ratings))))
      .map(user => (User(user._1, user._2._1), User(user._1, user._2._2))) // single user training and test ratings
      .unzip
  }

  def splitData(usersRatings: RDD[User], k: Int = 5, testDataRatio: Double = 0.2): List[ValidationDataSplit] = {
    val sc: SparkContext = usersRatings.sparkContext
    val groupedPartitions: List[List[User]] = splitUsersIntoFolds(usersRatings.collect(), k)

    val validationExamples = for (i <- 0 to k) yield {
      val testUsersSet = groupedPartitions.apply(i)
      val basicTrainingSet = groupedPartitions.zipWithIndex.filter(_._2 != i).flatMap(_._1)

      val (additionalTrainingSet, finalTestSet) = splitByTime(testUsersSet, 1 - testDataRatio)

      ValidationDataSplit(sc.parallelize(finalTestSet), sc.parallelize(basicTrainingSet ::: additionalTrainingSet))
    }

    validationExamples.toList
  }

  def splitUsersIntoFolds(usersRatings: Seq[User], k: Int): List[List[User]] = {
    val users = usersRatings.toList
    val usersCountPerBlock = users.size / k
    val groupedPartitions = users.grouped(usersCountPerBlock).toList
    groupedPartitions
  }
}

