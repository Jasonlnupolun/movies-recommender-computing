package org.miejski.recommendations.evaluation.partitioning

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.User

case class ValidationDataSplit(testData: RDD[User], trainingData: RDD[User])
