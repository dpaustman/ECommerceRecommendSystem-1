package xyz.jianzha.itemcf

/** Product 数据集
 *
 * @param userId
 * @param productId 商品ID
 * @param score
 * @param timestamp
 * @author Y_Kevin
 * @date 2020-10-19 10:34
 */
case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)
