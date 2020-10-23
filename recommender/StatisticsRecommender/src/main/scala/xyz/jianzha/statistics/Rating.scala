package xyz.jianzha.statistics

/**
 * Rating 数据集
 *
 * @param userId    用户ID
 * @param productId 商品ID
 * @param score     评分
 * @param timestamp 时间戳
 * @author Y_Kevin
 * @date 2020-10-19 10:36
 */
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)
