package xyz.jianzha.recommender

/**
 * MongoDB连接配置
 *
 * @param uri MongoDB的连接uri
 * @param db  要操作的db
 * @author Y_Kevin
 * @date 2020-10-19 10:36
 */
case class MongoConfig(uri: String, db: String)
