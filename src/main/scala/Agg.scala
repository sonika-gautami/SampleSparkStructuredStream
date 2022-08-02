import org.apache.spark.sql.SparkSession

object Agg extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("TestDs")
    .getOrCreate()

  val dfIn = spark.read
    .option("header", true)
    .csv("file:///Users/sonikagautami/My_NonBackup/GITHUB/SampleSparkStructuredStream/src/main/resources/youtube_trending_mediterranean.csv")
  dfIn.printSchema()

  val df = dfIn.na.drop(10)

  import org.apache.spark.sql.functions._

  df.agg(
    count("channelId"),
    count("country"),
    count("*")
  ).show()
  df.agg(
    countDistinct("channelId"),
    countDistinct("country"),
    countDistinct("*")
  ).show()

  df.select(
    approx_count_distinct("channelId", 0.3)
  ).show()

  df.where("length(country) == 2")
    .groupBy("country", "categoryId")
    .agg(sum("view_count"))
    .show()

  df.select(
    stddev("view_count"),
    covar_pop("view_count", "categoryId"),
    variance("view_count")
  ).show()

  df.filter("length(country) == 2")
    .groupBy("channelTitle")
    .pivot("country")
    .agg(
      sum("view_count"),
      sum("comment_count")
    ).show()

  df.groupBy("categoryId")
    .pivot("country", Seq("ES", "IT"))
    .agg(
      sum("view_count"),
      sum("comment_count")
    ).show()


  //country,video_id,title,publishedAt,channelId,channelTitle,categoryId,
  // trending_date,tags,view_count,comment_count,thumbnail_link,comments_disabled,
  // ratings_disabled,description
}
