package org.miejski.recommendations

import org.miejski.recommendations.helper.ShortCaseClasses
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}

trait TestSuiteTemplate extends FunSuite with Matchers
  with ShortCaseClasses
  with MockFactory {

}
