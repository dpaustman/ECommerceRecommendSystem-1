package xyz.jianzha.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @author Y_Kevin
 * @date 2020-10-18 21:00
 */
object DataLoader {
  // 定义数据文件路径
  val PRODUCT_DATA_PATH = "G:\\Workspaces\\workspaces_IDEA\\ECommerceRecommendSystem\\recommender\\dataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "G:\\Workspaces\\workspaces_IDEA\\ECommerceRecommendSystem\\recommender\\dataLoader\\src\\main\\resources\\ratings.csv"
  // 定义mongodb存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    // 配置文件数据
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.1.130:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // 创建Spark session
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    // 加载数据
    val productRDD: RDD[String] = sparkSession.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF: DataFrame = productRDD.map(item => {
      // product数据通过^分隔,切分出来
      val attr: Array[String] = item.split("\\^")
      // 转换成 Product
      Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val ratingRDD: RDD[String] = sparkSession.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF: DataFrame = ratingRDD.map(item => {
      // product数据通过^分隔,切分出来
      val attr: Array[String] = item.split(",")
      // 转换成 Rating
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    storeDataInMongoDB(productDF, ratingDF)

    sparkSession.stop()
  }

  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 新建一个mongoDB的连接, 客户端
    val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 定义要操作额mongo表
    val productColletion: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingColletion: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    // 如果表已经存在，则删除
    productColletion.dropCollection()
    ratingColletion.dropCollection()

    // 将当前数据存入对应的表中
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对表创建索引
    productColletion.createIndex(MongoDBObject("productId" -> 1))
    ratingColletion.createIndex(MongoDBObject("productId" -> 1))
    ratingColletion.createIndex(MongoDBObject("userId" -> 1))

    mongoClient.close()
  }
}
