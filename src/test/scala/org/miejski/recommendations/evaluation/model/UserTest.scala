package org.miejski.recommendations.evaluation.model

import org.miejski.recommendations.TestSuiteTemplate

class UserTest extends TestSuiteTemplate {

  test("should calculate mean rating") {
    val user = User("id", List(mr("1", 1.5), mr("2", 2.5), mr("3", 5.0)))

    user.meanRating() shouldBe 3.0
  }
}
