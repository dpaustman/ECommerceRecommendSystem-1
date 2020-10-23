package xyz.jianzha.recommender

/** Product 数据集
 *
 * @param productId  商品ID
 * @param name       商品名称
 * @param imageUrl   商品图片
 * @param categories 商品分类
 * @param tags       商品UDC标签
 * @author Y_Kevin
 * @date 2020-10-19 10:34
 */
case class Product(productId: Int, name: String, imageUrl: String, categories: String, tags: String)
