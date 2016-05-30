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
      .returns(Seq((m("m7"), 4.0), (m("m8"), 3.8), (m("m8"), 4.5), (m("m8"), 5.0), (m("m4"), 4.2)))

    val trainingData: RDD[User] = sc.parallelize(List(User("1", List(mr("m1", 3.0), mr("m2", 4.0), mr("m3", 5.0)))))
    val testData = sc.parallelize(List(new User("1", List(mr("m4", 3.0), mr("m4", 4.0)))))

    // when
    rankMetric.updateMetrics(recommender, new ValidationDataSplit(testData, trainingData))

    // then
    rankMetric.rankForUser should contain(("1", 0.2))
  }


  test("should take first item liked to rank metric") {
    // given

    // when


    // then

  }
}


