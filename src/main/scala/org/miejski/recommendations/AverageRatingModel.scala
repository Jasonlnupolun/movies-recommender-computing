package org.miejski.recommendations

import org.apache.spark.{SparkConf, SparkContext}
import org.miejski.recommendations.evaluation.partitioning.RandomPartitioner
import org.miejski.recommendations.recommendation.AverageRatingRecommender

object AverageRatingModel {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("MovieLensBuiltInCF")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "6g")
      .set("ALS.checkpointingInterval", "2")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    List(1, 2)

    sc.setCheckpointDir("checkpoint/")

    new ModelEvaluatorRunner().runSimulation(
      sc,
      new RandomPartitioner,
      (neighbours, ratings) => AverageRatingRecommender(ratings))

    sc.stop()

  }
}
