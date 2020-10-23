package xyz.jianzha.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Y_Kevin
 * @date 2020-10-19 10:39
 */
object StatisticsRecommender {
  // 定义mongodb存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.1.130:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    // 创建Spark session
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据
    val ratingDF: DataFrame = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    // 创建一张叫 ratings 的临时表
    ratingDF.createOrReplaceTempView("ratings")

    // 1.历史热门商品,按照评分个数统计
    val rateMoreProductsDF: DataFrame = sparkSession.sql("select productId, count(productId) as count from ratings group by productId order by count desc")
    storeDFInMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)

    // 2.近期热门商品,把时间戳转换成yyyyMM格式进行评分个数统计，最终得到productId，score，yearmonth
    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册UDF,将timestamp转化为年月格式yyyyMM
    sparkSession.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    // 把原始rating数据转换成想要的结构 productId，score，yearmonth
    val ratingOfYearMonthDF: DataFrame = sparkSession.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF: DataFrame = sparkSession.sql("select productId, count(productId) as count, yearmonth from ratingOfMonth " +
      "group by yearmonth, productId order by yearmonth desc, count desc")
    // 把 df 保存到mongodb
    storeDFInMongoDB(rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)

    // 3.优质商品统计,商品的平均评分
    val averageProductsDF: DataFrame = sparkSession.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    storeDFInMongoDB(averageProductsDF, AVERAGE_PRODUCTS)

    sparkSession.stop()
  }

  /**
   * 将数据写进MongoDB
   *
   * @param df              数据
   * @param collection_name 表名
   * @param mongoConfig     配置文件
   */
  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
