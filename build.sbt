name := "movies-recommender-computing"

version := "1.0"

scalaVersion := "2.11.7"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0-M15" % "test"

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.3.0"

// http://mvnrepository.com/artifact/org.scalamock/scalamock-scalatest-support_2.11
//libraryDependencies += "org.scalamock" % "scalamock-scalatest-support_2.11" % "3.2.2"
libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test"