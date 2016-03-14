package org.miejski.recommendations.neighbours

import org.miejski.recommendations.SparkSuite
import org.miejski.recommendations.evaluation.model.User

class NeighboursTest extends SparkSuite {

  val noWeightingFunction = (correlation: Double, b: Int, c: Int) => {
    correlation
  }

  test("calculating neighbours for user") {
    //given
    val user1 = User("1", List(mr("1", 5.0), mr("2", 5.0), mr("4", 4.5)))
    val user2 = User("2", List(mr("2", 3.0), mr("3", 5.0)))
    val user3 = User("3", List(mr("2", 3.0), mr("3", 5.0), mr("4", 5.0), mr("5", 3.0), mr("6", 2.0)))
    val user4 = User("4", List(mr("6", 4.0)))

    val users = sc.parallelize(List(user1, user2, user3, user4))

    //when
    val neighbours = Neighbours.fromUsers(users, noWeightingFunction)

    //then
    val neighboursInfo = neighbours.findFor("3")
    neighboursInfo should contain allOf(
      NeighbourInfo("4", 1.0, 4.0, 4.0),
      NeighbourInfo("2", 1.0, 4.0, 4.0),
      NeighbourInfo("1", -1.0, 4.75, 4.833333333333333))
  }
}
