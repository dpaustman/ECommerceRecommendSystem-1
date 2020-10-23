package xyz.jianzha.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
 * @author Y_Kevin
 * @date 2020-10-19 19:20
 */
object OfflineRecommender {
  // 定义mongodb存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20

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
    val ratingRDD: RDD[(Int, Int, Double)] = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => (rating.userId, rating.productId, rating.score))
      .cache()

    // 提取出所有用户和商品的数据集 distinct去重
    val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct()
    val productRDD: RDD[Int] = ratingRDD.map(_._2).distinct()

    // 1. 训练隐语义模型
    val trainData: RDD[Rating] = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    // 定义模型训练的参数，rank是隐特征个数, iterations 是迭代的次数, lambda 是ALS的正则化系数
    val (rank, iterations, lambda) = (50, 5, 0.01)
    // 调用ALS算法训练隐语义模型
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)

    // 2. 获取预测评分矩阵，得到用户推荐列表
    // 用 userRDD 和 productRDD 做一个笛卡尔积，得到空的userProductsRDD
    val userProducts: RDD[(Int, Int)] = userRDD.cartesian(productRDD)
    // model已训练好，把id传进去就可以得到预测评分列表RDD[Rating] (userId,productId,rating)
    val preRating: RDD[Rating] = model.predict(userProducts)

    // 从预测评分矩阵中提取得到用户推荐列表
    val userRecs: DataFrame = preRating.filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (userId, recs) =>
          UserRecs(
            userId,
            recs.toList
              .sortWith(_._2 > _._2)
              .take(USER_MAX_RECOMMENDATION)
              .map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 3. 利用商品的特征向量，计算商品的相似度列表
    val productFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
      case (productId, features) => (productId, new DoubleMatrix(features))
    }
    // 两两匹配商品，计算余弦相似度
    val productRecs: DataFrame = productFeatures.cartesian(productFeatures)
      .filter {
        case (a, b) => a._1 != b._1
      }
      //计算余弦相似度
      .map {
        case (a, b) =>
          val simScore: Double = consinSim(a._2, b._2)
          (a._1, (b._1, simScore))
      }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map {
        case (productId, recs) =>
          ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    sparkSession.stop()
  }

  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    product1.dot(product2) / (product1.norm2() * product2.norm2())
  }
}
