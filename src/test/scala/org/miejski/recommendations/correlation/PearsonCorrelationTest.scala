package org.miejski.recommendations.correlation

import org.miejski.recommendations.helper.ShortCaseClasses
import org.scalatest.{FunSuite, Matchers}

class PearsonCorrelationTest extends FunSuite
  with Matchers
  with ShortCaseClasses {

  val noWeightingFunction = (correlation: Double, b: Int, c: Int) => {
    correlation
  }

  test("should properly count correlation") {
    val first = List(mr("2", 2.0), mr("3", 5.0), mr("4", 7.0), mr("5", 1.0), mr("6", 8.0))
    val second = List(mr("1", 2.0), mr("2", 4.0), mr("3", 3.0), mr("4", 6.0), mr("5", 1.0), mr("6", 5.0))

    PearsonCorrelation.compute(first, second, noWeightingFunction) shouldEqual NeighboursDetails(0.793, 4.6, 3.8, 4.6, 3.5)
  }

  test("should properly compute 2 same dataset") {
    val data = List(mr("1", 2.0), mr("2", 5.0), mr("3", 7.0), mr("4", 1.0), mr("5", 8.0))

    PearsonCorrelation.compute(data, data, noWeightingFunction) shouldEqual NeighboursDetails(1.0, 4.6, 4.6, 4.6, 4.6)
  }

  test("should say -1 at different data") {
    val first = List(mr("1", 2.0), mr("2", 0.0))
    val second = List(mr("1", -1.0), mr("2", 6.0))

    PearsonCorrelation.compute(first, second, noWeightingFunction) shouldEqual NeighboursDetails(-1.0, 1.0, 2.5, 1.0, 2.5)
  }

  test("should properly compute correlation") {
    val first = List(mr("2", 5.0), mr("3", 2.0), mr("4", 8.0), mr("5", 7.0), mr("6", 1.0))
    val second = List(mr("1", 2.0), mr("2", 3.0), mr("3", 4.0), mr("4", 5.0), mr("5", 6.0), mr("6", 1.0))

    PearsonCorrelation.compute(first, second, noWeightingFunction) shouldEqual NeighboursDetails(0.793, 4.6, 3.8, 4.6, 3.5)
  }

  test("should properly compute 2 same dataset correlation") {
    val data = List(mr("1", 2.0), mr("23", 5.0), mr("8", 8.0), mr("22", 1.0), mr("7", 7.0))
    PearsonCorrelation.compute(data, data, noWeightingFunction) shouldEqual NeighboursDetails(1.0, 4.6, 4.6, 4.6, 4.6)
  }

  test("should say -1 at different data correlation") {
    val first = List(mr("1", 2.0), mr("2", 0.0))
    val second = List(mr("1", -1.0), mr("2", 6.0))

    PearsonCorrelation.compute(first, second, noWeightingFunction) shouldEqual NeighboursDetails(-1.0, 1.0, 2.5, 1.0, 2.5)
  }

  test("should say -1 when no common movies rated") {
    val first = List(mr("1", 2.0), mr("2", 0.0))
    val second = List(mr("3", -1.0), mr("4", 6.0))

    PearsonCorrelation.compute(first, second) shouldEqual NeighboursDetails(-1.0, 0.0, 0.0, 1.0, 2.5)
  }

  test("should properly apply significance weighting") {
    val weightedCorrelation = PearsonCorrelation.significanceWeighting(1.0, 1, 10)

    weightedCorrelation shouldEqual 0.1
  }

  def toOptional[T](a: Seq[T]): Seq[Option[Double]] = {
    a.collect {
      case null => None
      case x: Double => Some(x)
    }
  }
}
