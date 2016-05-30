package org.miejski.recommendations.evaluation.rank

import org.apache.spark.rdd.RDD
import org.miejski.recommendations._
import org.miejski.recommendations.evaluation.model.User
import org.miejski.recommendations.evaluation.partitioning.ValidationDataSplit
import org.miejski.recommendations.recommendation.MovieRecommender

class MeanReciprocalRankMetricTest extends SparkSuite {

  test("should calculate rank correctly when user really liked not the first movie from recommended set") {
    // given
    val rankMetric = new MeanReciprocalRankMetric()
    val recommender = stub[MovieRecommender]

    (recommender.forUser _).when("1", *)
      .returns(Seq((m("m7"), 4.0), (m("m8"), 3.8), (m("m5"), 4.5), (m("m10"), 5.0), (m("m4"), 2.7)))

    val trainingData: RDD[User] = sc.parallelize(List(User("1", List(mr("m1", 3.0), mr("m2", 4.0), mr("m3", 5.0)))))
    val testData = sc.parallelize(List(new User("1", List(mr("m4", 4.2), mr("m5", 3.0)))))

    // when
    rankMetric.updateMetrics(recommender, new ValidationDataSplit(testData, trainingData))

    // then
    rankMetric.rankForUser should contain(("1", 0.2))
  }

  test("should return 0 if no recommended movie has would really be liked by the user") {
    // given
    val rankMetric = new MeanReciprocalRankMetric()
    val recommender = stub[MovieRecommender]

    (recommender.forUser _).when("1", *)
      .returns(Seq((m("m7"), 4.0), (m("m8"), 3.8), (m("m5"), 4.5), (m("m10"), 5.0), (m("m4"), 2.7)))

    val trainingData: RDD[User] = sc.parallelize(List(User("1", List(mr("m1", 3.0), mr("m2", 4.0), mr("m3", 5.0)))))
    val testData = sc.parallelize(List(new User("1", List(mr("m4", 3.9999), mr("m5", 3.0)))))

    // when
    rankMetric.updateMetrics(recommender, new ValidationDataSplit(testData, trainingData))

    // then
    rankMetric.rankForUser should contain(("1", 0))
  }

  test("should return 0 if no recommended movie has really been watched by the user") {
    // given
    val rankMetric = new MeanReciprocalRankMetric()
    val recommender = stub[MovieRecommender]

    (recommender.forUser _).when("1", *)
      .returns(Seq((m("m7"), 4.0), (m("m8"), 3.8), (m("m5"), 4.5), (m("m10"), 5.0), (m("m4"), 3.7)))

    val trainingData: RDD[User] = sc.parallelize(List(User("1", List(mr("m1", 3.0), mr("m2", 4.0), mr("m3", 5.0)))))
    val testData = sc.parallelize(List(new User("1", List(mr("m40", 3.0), mr("m50", 4.0)))))

    // when
    rankMetric.updateMetrics(recommender, new ValidationDataSplit(testData, trainingData))

    // then
    rankMetric.rankForUser should contain(("1", 0.0))
  }
}


