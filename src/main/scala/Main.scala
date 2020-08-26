import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Main extends App {

  val inputDir = "file:///Users/sonikagautami/Learnings/SampleSparkStructured/src/main/resources/inputDir"

  val checkPointDir = "file:///Users/sonikagautami/Learnings/SampleSparkStructured/src/main/resources/checkPoints"

  val outputDir = "file:///Users/sonikagautami/Learnings/SampleSparkStructured/src/main/resources/outputDir"

  import scala.concurrent.duration._

  val windowDuration = 5.minutes
  val waterMarkDuration = 2.minutes

  val spark =
    SparkSession.builder()
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "1")
      .master("local")
      .getOrCreate()

  val schema =
    StructType(
      StructField("col1", StringType, true) ::
        StructField("col2", LongType, false) ::
        StructField("col3", LongType, false) :: Nil)

  val dfS: DataFrame =
    spark.readStream.schema(schema).csv(inputDir)

  private val dfWithTS = dfS
    .withColumn("tsz", concat(col("col1"), lit("UTC")))
    .withColumn("ts", to_timestamp(col("tsz"), "yyyy-MM-dd'T'HH:mm:ssz"))

  dfWithTS.printSchema()

  val printQ1: StreamingQuery =
    dfWithTS.writeStream.trigger(Trigger.Once()).format("console").start()
  printQ1.awaitTermination()


  val aggQueryDF =
    dfWithTS
      .withWatermark("ts", waterMarkDuration.toString)
      .groupBy(window(col("ts"), windowDuration.toString))
      .agg(
        sum("col2"),
        max("col3")
      )

  dfWithTS.printSchema()


  val finalDF =
    aggQueryDF
      .writeStream
      .trigger(Trigger.Once)
      .format("json")
      .partitionBy("window")
      .option("checkpointLocation", checkPointDir)
      .option("path", outputDir)
      .start()

  finalDF.awaitTermination()

  println(finalDF.lastProgress)
}
