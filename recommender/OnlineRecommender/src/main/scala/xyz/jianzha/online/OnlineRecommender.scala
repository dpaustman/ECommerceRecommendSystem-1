package xyz.jianzha.online

import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @author Y_Kevin
 * @date 2020-10-20 18:56
 */
object OnlineRecommender {
  // 定义常量和表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val STREAM_RECS = "StreamRecs"
  val PRODUCT_RECS = "ProductRecs"
  val MAX_USER_RATING_NUM = 20
  val MAX_USER_PRODUCTS_NUM = 20

  def main(args: Array[String]): Unit = {
    // 定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.1.130:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    // 创建spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    // 创建spark session
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import sparkSession.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据，相似度矩阵，广播出去
    val simProductsMatrix: collection.Map[Int, Map[Int, Double]] = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      // 为了后续查询相似度方便，把数据转换成map形式
      .map { item =>
        (item.productId, item.recs.map(x => (x.productId, x.score)).toMap)
      }
      .collectAsMap()

    // 定义广播变量
    val simProductsMatrixBC: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simProductsMatrix)

    // 创建Kafka配置参数
    val kafkaParam = Map(
      "bootstrap.servers" -> "hadoop101:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    // 创建一个 DStream
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
    )

    // 对kafkaStream 进行处理，产生评分流， userId|productId|score|timestamp
    val ratingStream: DStream[(Int, Int, Int, Int)] = kafkaStream.map { msg =>
      var attr: Array[String] = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toInt, attr(3).toInt)
    }

    // 核心算法部分，定义评分流的处理流程
    ratingStream.foreachRDD {
      rdds =>
        rdds.foreach {
          case (userId, productId, score, timestamp) =>
            println("rating data coming! >>>>>>>>>>>>>>>>>>>>>>>")

            // 1.从redis里取出当前用户的最近评分，保存成一个数组 Array[(productId, score)]
            val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRatings(MAX_USER_RATING_NUM, userId, ConnHelper.jedis)

            // 2.从相似度矩阵中获取当前商品最相似的商品列表，作为备选列表，保存成一个数组Array[productId]
            val candidateProduct: Array[Int] = getTopSimProducts(MAX_USER_PRODUCTS_NUM, productId, userId, simProductsMatrixBC.value)

            // 3.计算每一个备选商品的推荐优先级，得到当前用户的实时推荐列表，保存成 Array[(productId, score)]
            val streamRecs: Array[(Int, Double)] = computeProductScore(candidateProduct, userRecentlyRatings, simProductsMatrixBC.value)

            // 4.把推荐列表保存到mongodb
            saveDataToMongoDB(userId, streamRecs)
        }
    }

    // 启动
    ssc.start()
    println("streaming started!")
    ssc.awaitTermination()
  }

  import scala.collection.JavaConversions._

  /**
   * 从redis 里获取最近num次评分
   *
   * @param num    评分的个数
   * @param userId 谁的评分
   * @return
   */
  def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    // 从redis中用户的评分队列里获取评分数据，list 的键名为uid:USERID，值格式是 PRODUCTID:SCORE
    jedis.lrange("userId:" + userId.toString, 0, num)
      .map { item =>
        val attr: Array[String] = item.split(":")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  /**
   * 获取当前商品的相似列表，并过滤掉用户已经评分过的，作为备选列表
   *
   * @param num         相似商品的数量
   * @param productId   当前商品的ID
   * @param userId      当前的评分用户
   * @param simProducts 商品相似度矩阵的广播变量值
   * @param mongoConfig MongoDB的配置
   * @return
   */
  def getTopSimProducts(num: Int,
                        productId: Int,
                        userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int] = {
    // 从广播变量相似度矩阵中拿到当前商品的相似度列表
    val allSimProducts: Array[(Int, Double)] = simProducts(productId).toArray

    // 获得用户已经评分过的商品,过滤掉,排序输出
    val ratingColletion: MongoCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
    val ratingExist: Array[Int] = ratingColletion.find(MongoDBObject("userId" -> userId))
      .toArray
      .map { item => // 只需要productId
        item.get("productId").toString.toInt
      }
    // 从所有的相似商品中进行过滤
    allSimProducts.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  /**
   * 计算待选商品的推荐分数
   *
   * @param candidateProducts   当前商品最相似的K个商品
   * @param userRecentlyRatings 用户最近的k次评分
   * @param simProducts         商品相似度矩阵
   * @return
   */
  def computeProductScore(candidateProducts: Array[Int],
                          userRecentlyRatings: Array[(Int, Double)],
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  : Array[(Int, Double)] = {
    // 定义一个长度可变数组ArrayBuffer，用于保存每一个备选商品的基础得分，(productId, score)
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    // 定义两个map,用于保存每个商品的高分和低分的计数器, productId -> count
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    // 遍历每个备选商品，计算和已评分商品的相似度
    for (candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings) {
      // 从相似度矩阵中获取当前备选商品和当前已评分商品间的相似度
      val simScore: Double = getProductSimScore(candidateProduct, userRecentlyRating._1, simProducts)
      if (simScore > 0.4) {
        // 按照公式进行加权计算，得到基础评分
        scores += ((candidateProduct, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increMap(candidateProduct) = increMap.getOrDefault(candidateProduct, 0) + 1
        } else {
          decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct, 0) + 1
        }
      }
    }

    // 根据公式计算所有的推荐优先级，首先以productId做groupby
    scores.groupBy(_._1).map {
      case (productId, scoreList) =>
        (productId, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(productId, 1)) - log(decreMap.getOrDefault(productId, 1)))
    }
      // 返回推荐列表，按照得分排序
      .toArray
      .sortWith(_._2 > _._2)
  }

  /**
   * 获取当个商品之间的相似度
   *
   * @param simProducts       商品相似度矩阵
   * @param userRatingProduct 用户已经评分的商品
   * @param topSimProduct     候选商品
   * @return
   */
  def getProductSimScore(topSimProduct: Int,
                         userRatingProduct: Int,
                         simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {

    simProducts.get(topSimProduct) match {
      case Some(sims) => sims.get(userRatingProduct) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // 自定义log函数，以N为底
  def log(m: Int): Double = {
    val N = 10
    math.log(m) / math.log(N)
  }

  // 写入mongoDB
  def saveDataToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    val streamRecsCollection: MongoCollection = ConnHelper.mongoClient(mongoConfig.db)(STREAM_RECS)
    // 按照userId查询并更新
    streamRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streamRecsCollection.insert(MongoDBObject("userId" -> userId, "recs" -> streamRecs.map(x => MongoDBObject("productId" -> x._1, "score" -> x._2))))
  }
}
