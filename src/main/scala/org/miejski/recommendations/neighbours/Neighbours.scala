package org.miejski.recommendations.neighbours

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.correlation.{NeighboursDetails, PearsonCorrelation}
import org.miejski.recommendations.evaluation.model.User

case class Neighbours(topNeighbours: Map[String, Seq[NeighbourInfo]]) extends Serializable

object Neighbours {

  def findFor(neighbours: Neighbours, userId: String, top: Int = 5): List[NeighbourInfo] = {
    implicit val ord: Ordering[NeighbourInfo] = new Ordering[NeighbourInfo] {
      override def compare(x: NeighbourInfo, y: NeighbourInfo): Int = y.similarity.compare(x.similarity)
    }
    neighbours.topNeighbours.getOrElse(userId, Seq.empty).sorted.take(top).toList
  }

  def getUserAverageRating(neighbours: Neighbours, user: String) = {
    val topNeighbours = neighbours.topNeighbours

    topNeighbours.filter(s => !s._1.equals(user))
      .map(s => s._2.filter(_.neighbourName.equals(user)).head)
      .map(s => UserAverageRating(s.neighbourName, s.neighbourAverageRating))
      .head
  }

  def printNeighbours(neighbours: Neighbours, user: String, top: Int = 5) = {
    println(s"Neighbours for user: $user")
    findFor(neighbours, user).take(top).foreach(println)
  }

  def sameUsers: ((User, User)) => Boolean = { s => !s._1.id.equals(s._2.id) }

  def fromUsers(userRatings: RDD[User],
                weighting: (Double, Int, Int) => Double = PearsonCorrelation.significanceWeighting): Neighbours = {

    val joinedUsers = userRatings.cartesian(userRatings).filter(sameUsers).cache()
    //    joinedUsers.collect()
    val uniqueUsersPairs: RDD[(String, String)] = joinedUsers.map(r => (r._1.id, r._2.id))
      .filter(x => x._1 < x._2)

    val uniqueMappings = uniqueUsersPairs.map((_, None)) // for join with correlations

    val correlations = joinedUsers.map(ratings => (
      (ratings._1.id, ratings._2.id),
      PearsonCorrelation.compute(ratings._1.ratings, ratings._2.ratings, weighting)))
    val uniqueUsersCorrelations = uniqueMappings.join(correlations).map(s => (s._1, s._2._2))

    val topNeighbours: RDD[(String, Seq[NeighbourInfo])] = bidirectionalNeighboursMapping(uniqueUsersCorrelations)

    //    topNeighbours.collect()
    topNeighbours.collect()
    new Neighbours(null)
  }

  def fromUsersNoRdd(userRatings: Seq[(User, User)],
                     weighting: (Double, Int, Int) => Double = PearsonCorrelation.significanceWeighting): Neighbours = {
    val a = userRatings.map(x => ((x._1.id, x._2.id), PearsonCorrelation.compute(x._1.ratings, x._2.ratings, weighting)))
    val topNeighboursMap = bidirectionalNeighboursMappingNoRdd(a)
    new Neighbours(topNeighboursMap)
  }

  def bidirectionalNeighboursMappingNoRdd(uniqueUsersCorrelations: Seq[((String, String), NeighboursDetails)]): Map[String, Seq[NeighbourInfo]] = {
    val map = uniqueUsersCorrelations.filter(c => !c._1._1.equals(c._1._2))
      .flatMap(corr => Seq(
        (corr._1._1, NeighbourInfo(corr._1._2, corr._2.similarity, corr._2.user2SharedAverageRating, corr._2.user2AverageRating)),
        (corr._1._2, NeighbourInfo(corr._1._1, corr._2.similarity, corr._2.user1SharedAverageRating, corr._2.user1AverageRating))))

    val r = map.groupBy(_._1).mapValues(s => s.map(x => x._2).sortBy(singleCorr => singleCorr.similarity).reverse)
    r
  }

  def bidirectionalNeighboursMapping(uniqueUsersCorrelations: RDD[((String, String), NeighboursDetails)]): RDD[(String, Seq[NeighbourInfo])] = {
    val map: RDD[(String, NeighbourInfo)] = uniqueUsersCorrelations.filter(c => !c._1._1.equals(c._1._2))
      .flatMap(corr => Seq(
        (corr._1._1, NeighbourInfo(corr._1._2, corr._2.similarity, corr._2.user2SharedAverageRating, corr._2.user2AverageRating)),
        (corr._1._2, NeighbourInfo(corr._1._1, corr._2.similarity, corr._2.user1SharedAverageRating, corr._2.user1AverageRating))))
    val topNeighbours = map
      .groupByKey()
      .map(s => (s._1, s._2.toSeq.sortBy(singleCorr => singleCorr.similarity).reverse))
    topNeighbours
  }
}

case class NeighbourInfo(neighbourName: String,
                         similarity: Double,
                         neighbourSharedAverageRating: Double,
                         neighbourAverageRating: Double) extends Serializable

case class UserAverageRating(user: String, averageRating: Double)