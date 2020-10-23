package xyz.jianzha.itemcf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Y_Kevin
 * @date 2020-10-22 13:37
 */
object ItemCFRecommender {
  // 定义常量和表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val ITEM_CF_PRODUCT_RECS = "ItemCFProductRecs"
  val MAX_RECOMMENDATION = 10

  def main(args: Array[String]): Unit = {
    // 定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.1.130:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ItemCFRecommender")
    // 创建spark session
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据，转换成DF进行处理
    val ratingDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .map(x => (x.userId, x.productId, x.score))
      .toDF("userId", "productId", "rating")
      .cache()

    // TODO :核心算法，计算同现相似度，得到商品的相似度列表
    // 统计每个商品的评分个数，按照productId来做group by
    val productRatingCountDF: DataFrame = ratingDF.groupBy("productId").count()
    // 在原有的评分表上rating添加count
    val ratingWithCountDF: DataFrame = ratingDF.join(productRatingCountDF, "productId")

    // 将评分按照用户ID两两配对，统计两个商品被同一个用户评分过的次数
    val joinedDF = ratingWithCountDF.join(ratingWithCountDF, "userId")
      .toDF("userId", "product1", "score1", "count1", "product2", "score2", "count2")
      .select("userId", "product1", "count1", "product2", "count2")
    // 创建一张临时表，用于写SQL查询
    joinedDF.createOrReplaceTempView("joined")

    // 按照product1，product2 做group by，统计userId的数量，就是对两个商品同时评分的人数
    val cooccurrenceDF = sparkSession.sql(
      """
        |select product1
        |, product2
        |, count(userId) as cocount
        |, first(count1) as count1
        |, first(sount2) as count2
        |from joined
        |group by product1, product2
        |""".stripMargin
    ).cache()

    // 提取需要的数据，包装成（productId1, (productId2,score))
    val simDF = cooccurrenceDF.map {
      row =>
        val coocSim = cooccurrenceSim(row.getAs[Long]("cocount"), row.getAs[Long]("count1"), row.getAs[Long]("count2"))
        (row.getInt(0), (row.getInt(1), coocSim))
    }
      .rdd
      .groupByKey()
      .map {
        case (productId, recs) =>
          ProductRecs(productId,
            recs.toList
              .filter(x => x._1 != productId)
              .sortWith(_._2 > _._2)
              .map(x => Recommendation(x._1, x._2))
              .take(MAX_RECOMMENDATION)
          )
      }
      .toDF()

    // 保存到mongodb
    simDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", ITEM_CF_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.soark.sql")
      .save()

    sparkSession.stop()
  }

  // 按照公式计算同现相似度
  def cooccurrenceSim(cooCount: Long, count1: Long, count2: Long): Double = {
    cooCount / math.sqrt(count1 * count2)
  }
}
