package org.miejski.recommendations.evaluation.partitioning

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.User

trait Partitioner {
  def splitData(usersRatings: RDD[User], k: Int = 5, testDataRatio: Double = 0.2): List[ValidationDataSplit]
}
