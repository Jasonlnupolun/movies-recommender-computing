package org.miejski.recommendations

import org.apache.spark.{SparkConf, SparkContext}

trait SparkSuite extends TestSuiteTemplate {
  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("SparkSuite").setMaster("local[4]"))

}
