package xyz.jianzha.offline

/**
 * 用户推荐列表
 *
 * @author Y_Kevin
 * @date 2020-10-19 20:56
 */
case class UserRecs(userId: Int, recs: Seq[Recommendation])
