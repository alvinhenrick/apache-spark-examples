package com.yelp.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive._

/**
  * Created by Alvin on 7/4/15.
  */
object AnalyzeJSONWithDataFrame {

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local[*]"
    }
    val sc = new SparkContext(master, "AnalyzeJSONWithDataFrame")

    val sqlContext = new SQLContext(sc)

    val input_file = "/Users/shona/analytics/data/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json"
    val jsonData = sc.textFile(input_file)

    val businessReviews = sqlContext.read.json(jsonData).cache()
    businessReviews.registerTempTable("businessReviews")


    val firstRecordQuery =
      """SELECT
        | *
        | FROM businessReviews
        | limit 1
      """.stripMargin

    val firstRecord = sqlContext.sql(firstRecordQuery)

    firstRecord.show()

    val countByCityStateQuery =
      """SELECT
        | state, city, COUNT(*) as totalReviews
        | FROM businessReviews
        | GROUP BY state, city ORDER BY totalReviews DESC LIMIT 10
        | """.stripMargin


    val countByCityState = sqlContext.sql(countByCityStateQuery)

    countByCityState.show()

    val truncateUDF = (n: Double, p: Int) => {
      val s = math pow(10, p)
      (math floor n * s) / s
    }

    sqlContext.udf.register("truncate", truncateUDF)


    val avgReviewRatingQuery =
      """SELECT
        |  stars,truncate(AVG(review_count),0) as reviewsAvg
        |  from businessReviews
        |  GROUP BY stars ORDER BY stars DESC
        | """.stripMargin


    val avgReviewRating = sqlContext.sql(avgReviewRatingQuery)

    avgReviewRating.show()


    val topBusinessQuery =
      """SELECT
        |  name,state, city,review_count
        |  from businessReviews
        |  WHERE review_count > 1000
        |  ORDER BY review_count DESC LIMIT 10
        | """.stripMargin

    val topBusiness = sqlContext.sql(topBusinessQuery)

    topBusiness.show()


    val satOpenCloseTimeQuery =
      """SELECT
        |  name,hours.Saturday.open as open, hours.Saturday.close as close
        |  from businessReviews
        |  LIMIT 10
        | """.stripMargin

    val satOpenCloseTime = sqlContext.sql(satOpenCloseTimeQuery)

    satOpenCloseTime.show()

    val hiveCtx = new HiveContext(sc)

    val businessReviewsHive = hiveCtx.read.json(jsonData).cache()

    businessReviewsHive.registerTempTable("businessReviews")

    val restaurantsQuery =
      """SELECT
        |  name,state,city,review_count , cat
        |  FROM businessReviews
        |  LATERAL VIEW explode(categories) tab AS cat
        |  WHERE cat = 'Restaurants'
        |  ORDER BY review_count DESC LIMIT 10
        | """.stripMargin

    val restaurants = hiveCtx.sql(restaurantsQuery)

    restaurants.show()

    val restaurantsCategoryCountQuery =
      """SELECT
        |  name, SIZE(categories) as categoryCount, categories
        |  FROM businessReviews
        |  LATERAL VIEW explode(categories) tab AS cat
        |  WHERE cat = 'Restaurants'
        |  ORDER BY categoryCount DESC LIMIT 10
        | """.stripMargin

    val restaurantsCategoryCount = hiveCtx.sql(restaurantsCategoryCountQuery)

    restaurantsCategoryCount.show()

    val firstCategoryCountQuery =
      """SELECT
        |  categories[0] as firstCategory, COUNT(categories[0]) as categoryCount
        |  FROM businessReviews
        |  GROUP BY categories[0]
        |  ORDER BY categoryCount DESC LIMIT 10
        | """.stripMargin

    val firstCategoryCount = hiveCtx.sql(firstCategoryCountQuery)

    firstCategoryCount.show()


    val flattenCategoryQuery =
      """SELECT
        |  name,EXPLODE(categories) as category
        |  FROM businessReviews
        |  LIMIT 10
        | """.stripMargin

    val flattenCategory = hiveCtx.sql(flattenCategoryQuery)

    flattenCategory.show()



    val topCategoryBusinessReviewQuery =
      """SELECT
        |  tempTable.category, COUNT(tempTable.category) categorycnt
        |  FROM (SELECT EXPLODE(categories) category FROM businessReviews ) tempTable
        |  GROUP BY tempTable.category
        |  ORDER BY categorycnt DESC LIMIT 10
        | """.stripMargin

    val topCategoryBusinessReview = hiveCtx.sql(topCategoryBusinessReviewQuery)
    topCategoryBusinessReview.show()




    sc.stop()
  }

}
