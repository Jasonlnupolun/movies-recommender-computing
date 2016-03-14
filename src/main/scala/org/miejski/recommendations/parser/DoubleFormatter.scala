package org.miejski.recommendations.parser

trait DoubleFormatter {
  def format(value: Double, unknownValue : Double = 0.0, defaultValue: Double = 0.0 ): Double = {
    if ( value == unknownValue) defaultValue else "%.3f".format(value).toDouble
  }
}
