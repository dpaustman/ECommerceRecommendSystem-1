package xyz.jianzha.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Y_Kevin
 * @date 2020-10-20 10:11
 */
object ALSTrainer {
  // 定义mongodb存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    // 定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.1.130:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    // 创建spark session
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 读取mongoDB中的业务数据
    val ratingRDD: RDD[Rating] = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => Rating(rating.userId, rating.productId, rating.score))
      .cache()

    // 数据集切分成训练集和测试集
    val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD: RDD[Rating] = splits(0)
    val testingRDD: RDD[Rating] = splits(1)

    // 核心实现：输出最优参数
    adjustALSParams(trainingRDD, testingRDD)

    sparkSession.stop()
  }

  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    // 遍历数组中定义的参数取值
    val result: Array[(Int, Double, Double)] = for (rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01))
      yield {
        val model: MatrixFactorizationModel = ALS.train(trainData, rank, 10, lambda)
        val rmse: Double = getRMSE(model, testData)
        (rank, lambda, rmse)
      }

    // 按照 rmse 排序并输出最优参数
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // 构建 userProducts ,得到预测评分矩阵
    val userProducts: RDD[(Int, Int)] = data.map(item => (item.user, item.product))
    val predictRating: RDD[Rating] = model.predict(userProducts)

    // 按照公式计算rmse，首先把预测评分和实际评分表（userId，productId) 做一个连接
    val observed: RDD[((Int, Int), Double)] = data.map(item => ((item.user, item.product), item.rating))
    val predict: RDD[((Int, Int), Double)] = predictRating.map(item => ((item.user, item.product), item.rating))

    sqrt(
      observed.join(predict).map {
        case ((userId, productId), (actual, pre)) =>
          val err: Double = actual - pre
          err * err
      }.mean()
    )
  }
}
