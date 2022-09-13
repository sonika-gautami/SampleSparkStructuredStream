import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes

object Windows extends App {
  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  val dfIn = spark.read
    .option("header", true)
    .csv("file:///Users/sonikagautami/My_NonBackup/GITHUB/SampleSparkStructuredStream/src/main/resources/youtube_trending_mediterranean.csv")
    .na.drop(10)
    .where(length('country) === 2)
    .withColumn("view_count", 'view_count.cast(DataTypes.LongType))
    .withColumn("comment_count", 'comment_count.cast(DataTypes.LongType))

  dfIn.printSchema()
  dfIn.show()
  //country,video_id,title,publishedAt,channelId,channelTitle,categoryId,
  // trending_date,tags,view_count,comment_count,thumbnail_link,
  // comments_disabled,ratings_disabled,description

  dfIn.rollup('country, 'categoryId)
    .agg(sum('view_count), sum('comment_count))
    .orderBy(asc_nulls_first("country"))
    .show()
  dfIn.cube('country, 'categoryId)
    .agg(sum('view_count), sum('comment_count))
    .orderBy(asc_nulls_first("country"))
    .show()

  dfIn.select(
    'publishedAt,
    unix_timestamp('publishedAt),
    dayofyear('publishedAt)
  ).show()

  dfIn.groupBy(window('publishedAt, "7 day"))
    .agg(collect_list('categoryId) as "list")
    .select("window.start", "window.end", "list")
    .show(50, 200, false)

  dfIn.groupBy(window('publishedAt, "4 week", "1 week"))
    .agg(sum('view_count) as "views")
    .select("window.start", "window.end", "views")
    .sort("window.start")
    .show(2000, 200, false)

  val winExp1 = Window.partitionBy("country")
    .orderBy(desc("view_count"))

  dfIn.withColumn("rank", rank().over(winExp1))
    .withColumn("d_rank", dense_rank().over(winExp1))
    .selectExpr("rank", "d_rank", "country", "view_count")
    .orderBy('rank)
    .show(200, 200, false)

  //ntile -  ~ finding a bucket (Range)
  dfIn.withColumn("ntile", ntile(10) over (
    Window.partitionBy("country").orderBy("view_count")))
    .orderBy('country)
    .select("ntile", "country", "view_count")
    .show(200)

  dfIn.limit(200)
    .withColumn("per_rank", percent_rank() over Window
      .partitionBy("country").orderBy("comment_count"))
    .select("per_rank", "country", "comment_count")
    .show(200)

  dfIn.withColumn("row_number", rank() over(
    Window.partitionBy("id").orderBy("id")
  )).where("row_number == 1")
    .selectExpr("*")
    .show()

  /*
  lag -> the value prior to offset rows from DataFrame.
        For example,
        if lag offset is 2 and lag is applied on rank, then
        rank | lag_col
        1    | null
        2    | null
        3    | 1
        4    | 2
        5    | 3
  lead -> the value after to offset rows from DataFrame.
          For example,
          if lead offset is 2 and lead is applied on rank, then
          rank | lag_col
          1    | 3
          2    | 4
          3    | 5
          4    | null
          5    | null
   */
  dfIn.limit(20)
    .withColumn("lag", lag('comment_count, 2) over Window
      .partitionBy('country).orderBy('comment_count))
    .withColumn("lead", lead('comment_count, 2) over Window
      .partitionBy('country).orderBy('comment_count))
    .select("lead", "lag", "country", "comment_count")
    .show()

  /*
   rangesBetween(Range-Start (min) , Range-End (max)
   rowsBetween(Row-Num-Relative to current-row  (min, max)

   Window.currentRow: to specify a current value in a row.
   Window.unboundedPreceding: This can be used to have an unbounded start for the window.
   Window.unboundedFollowing: This can be used to have an unbounded end for the window.

   Default = rowsBetween(Window.unboundedPreceding, Window.currentRow)
   */
  /* GroupBy(columns) -> Aggregation(Columns)
     Any Operation on Row [operation] Aggregation Column
     For example, per category -> difference between
                        views of max per category &
                        views of row
   */
  val winExp3 = Window.partitionBy('categoryId)
    .orderBy('view_count.desc)
    .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  dfIn.limit(50)
    .withColumn("col1", (max('view_count) over winExp3) - 'view_count)
    .select("view_count", "col1", "categoryId")
    .orderBy('categoryId, 'view_count)
    .show(50)

  //cummulative sum
  val winExp4 = Window.partitionBy('categoryId)
    .orderBy('view_count)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  dfIn.limit(50)
    .withColumn("col1", sum('view_count) over winExp4)
    .select("view_count", "col1", "categoryId")
    .orderBy('categoryId, 'view_count)
    .show(50)



}
