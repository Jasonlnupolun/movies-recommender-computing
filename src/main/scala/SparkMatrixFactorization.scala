import org.apache.spark.{SparkContext, SparkConf}
import org.miejski.recommendations.{MatrixFactorizationRecommender, ModelEvaluatorRunner}
import org.miejski.recommendations.evaluation.CrossValidationPartitioner

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
      (dataSplitter) => new CrossValidationPartitioner().allCombinationsTimestampBased(dataSplitter),
      (neighbours, ratings) => MatrixFactorizationRecommender(ratings))

    sc.stop()
  }

}
