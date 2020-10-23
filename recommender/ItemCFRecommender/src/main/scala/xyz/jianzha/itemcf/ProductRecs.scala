package xyz.jianzha.itemcf

/**
 * 商品相似度列表（商品推荐）
 *
 * @author Y_Kevin
 * @date 2020-10-19 20:56
 */
case class ProductRecs(productId: Int, recs: Seq[Recommendation])
