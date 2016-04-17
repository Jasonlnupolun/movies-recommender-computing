package org.miejski.recommendations.neighbours

import org.miejski.recommendations.evaluation.model.User
import org.miejski.recommendations.helper.ShortCaseClasses
import org.scalatest.{FunSuite, Matchers}

class NeighboursTest extends FunSuite with Matchers
  with ShortCaseClasses {

  val noWeightingFunction = (correlation: Double, b: Int, c: Int) => {
    correlation
  }

  test("calculating neighbours for user") {
    //given
    val user1 = User("1", List(mr("1", 5.0), mr("2", 5.0), mr("4", 4.5)))
    val user2 = User("2", List(mr("2", 3.0), mr("3", 5.0)))
    val user3 = User("3", List(mr("2", 3.0), mr("3", 5.0), mr("4", 5.0), mr("5", 3.0), mr("6", 2.0)))
    val user4 = User("4", List(mr("6", 4.0)))

    val users = Seq(user1, user2, user3, user4)
      .combinations(2).map(x => (x.head, x.tail.head)).toSeq

    //when
    val neighbours = Neighbours.fromUsersNoRdd(users, noWeightingFunction)

    //then
    val neighboursInfo = Neighbours.findFor(neighbours, "3")
    neighboursInfo should contain allOf(
      NeighbourInfo("4", 1.0, 4.0, 4.0),
      NeighbourInfo("2", 1.0, 4.0, 4.0),
      NeighbourInfo("1", -1.0, 4.75, 4.833333333333333))
  }

  test("should return good neighbours for given user") {
    val neighbours = new Neighbours(Map(
      ("1", Seq(NeighbourInfo("3", 3.0, 3.0, 2.0),
        NeighbourInfo("4", 2.0, 2.0, 2.0),
        NeighbourInfo("5", 1.0, 1.0, 2.0))),
      ("2", Seq(NeighbourInfo("3", 3.0, 2.0, 3.5),
        NeighbourInfo("4", 4.0, 5.0, 3.0),
        NeighbourInfo("5", 2.0, 4.0, 3.0)))))


    Neighbours.findFor(neighbours, "1") should equal(Seq(NeighbourInfo("3", 3.0, 3.0, 2.0),
      NeighbourInfo("4", 2.0, 2.0, 2.0),
      NeighbourInfo("5", 1.0, 1.0, 2.0)))
    Neighbours.findFor(neighbours,"2") should equal(Seq(NeighbourInfo("4", 4.0, 5.0, 3.0),
      NeighbourInfo("3", 3.0, 2.0, 3.5),
      NeighbourInfo("5", 2.0, 4.0, 3.0)))

    Neighbours.findFor(neighbours,"2", 2) should equal(Seq(NeighbourInfo("4", 4.0, 5.0, 3.0),
      NeighbourInfo("3", 3.0, 2.0, 3.5)))
  }
}
