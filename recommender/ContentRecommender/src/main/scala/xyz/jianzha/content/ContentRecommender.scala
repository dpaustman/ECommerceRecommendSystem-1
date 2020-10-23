package xyz.jianzha.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
 * @author Y_Kevin
 * @date 2020-10-22 9:12
 */
object ContentRecommender {
  // 定义mongodb存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val CONTENT_PRODUCT_RECS = "ContentBasedProductRecs"

  def main(args: Array[String]): Unit = {
    // 定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.1.130:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")
    // 创建spark session
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 载入数据，做预处理
    val productTagsDF: DataFrame = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Product]
      .map(x => (x.productId, x.name, x.tags.map(c => if (c == '|') ' ' else c)))
      .toDF("productId", "name", "tags")
      .cache()

    // TODO 用TF-IDF提取商品特征向量
    // 1.实例化一个分词器，用来做分词
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    // 用分词器做转换，得到增加一个新列words的DF
    val wordDataDF: DataFrame = tokenizer.transform(productTagsDF)

    // 2.定义一个HashingTF工具，计算TF
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)
    val featurizedDataDF: DataFrame = hashingTF.transform(wordDataDF)

    // 3.定义一个IDF工具，计算TF-IDF
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //  训练一个idf模型
    val idModel: IDFModel = idf.fit(featurizedDataDF)
    // 得到增加新列feature 的DF
    val rescaledDataDF: DataFrame = idModel.transform(featurizedDataDF)

    rescaledDataDF.show(truncate = false)

    // 对数据进行转换，得到RDD形式的features
    val productFeatures: RDD[(Int, DoubleMatrix)] = rescaledDataDF.map {
      row => (row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray)
    }
      .rdd
      .map {
        case (productId, features) => (productId, new DoubleMatrix(features))
      }

    // 两两匹配商品，计算余弦相似度
    val productRecs: DataFrame = productFeatures.cartesian(productFeatures)
      .filter {
        case (a, b) => a._1 != b._1
      }
      //计算余弦相似度
      .map {
        case (a, b) =>
          val simScore: Double = consinSim(a._2, b._2)
          (a._1, (b._1, simScore))
      }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map {
        case (productId, recs) =>
          ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", CONTENT_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    sparkSession.stop()
  }

  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    product1.dot(product2) / (product1.norm2() * product2.norm2())
  }
}
