import org.apache.spark.{SparkConf, SparkContext}
import org.miejski.recommendations.evaluation.partitioning.CrossValidationPartitioner
import org.miejski.recommendations.{MatrixFactorizationRecommender, ModelEvaluatorRunner}

object SparkMatrixFactorization {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("MovieLensBuiltInCF")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "6g")
      .set("ALS.checkpointingInterval", "2")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("checkpoint/")

    new ModelEvaluatorRunner().runSimulation(
      sc,
      new CrossValidationPartitioner,
      (neighbours, ratings) => MatrixFactorizationRecommender(ratings))

    sc.stop()
  }

}
