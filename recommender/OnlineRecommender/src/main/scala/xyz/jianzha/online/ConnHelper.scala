package xyz.jianzha.online

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.Jedis

/**
 * 定义一个连接助手对象，建立到redis和mongodb的连接
 *
 * @author Y_Kevin
 * @date 2020-10-20 20:50
 */
object ConnHelper extends Serializable {
  // 懒变量定义，使用的时候才初始化
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient: MongoClient = MongoClient(MongoClientURI("mongodb://192.168.1.130:27017/recommender"))
}
