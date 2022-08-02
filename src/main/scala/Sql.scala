import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.nio.file.Paths

object Sql extends App {

  val seq = Seq("one", "two", "three", "four").map(s => (s, s.length))

  val spark = SparkSession.builder()
    .master("local")
    .appName("TestDs")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val df = spark.sparkContext.parallelize(seq).toDF("str", "value")
  df.printSchema()

  spark.catalog.listTables.show
  df.createOrReplaceTempView("testsql")
  println(
    spark.catalog.tableExists("testsql")
  )

  df.createOrReplaceGlobalTempView("testsqlGlobal")
  spark.catalog.listTables.show
  spark.sql(
    """
      |select * from global_temp.testsqlGlobal
      |""".stripMargin)
    .show()

  spark.catalog.dropGlobalTempView("testsqlGlobal")
  spark.catalog.listTables.show

  spark.sql(
    """
      |select unix_timestamp() as ts, collect_list(str) as arr, value
      |from testsql
      |where value < 5
      |group by value
      |""".stripMargin)
    .orderBy('value.desc)
    .show()

  spark.sql(
    """
      |select * from (
      |   select unix_timestamp() as ts, collect_list(str) as arr, value
      |   from testsql
      |   where value < 5
      |   group by value ) a
      |order by value DESC
      |""".stripMargin)
    .show()

  val df2 = spark.sql(
    """
      |select * from csv.`file:///Users/sonikagautami/My_NonBackup/GITHUB/SampleSparkStructuredStream/src/main/resources/inputDir`
      |""".stripMargin)
    .repartition(4)

  val rdd = df2.coalesce(2).rdd
  println(rdd.getStorageLevel)
  println(rdd.getNumPartitions)
  println(rdd.getResourceProfile())




  //bucketBy -> saveAsTable only supported
  df.write
    .bucketBy(2, "str")
    .sortBy("value")
    .format("json")
    .mode(SaveMode.Overwrite)
    .saveAsTable("testtableb1")

  spark.read.table("testtableb1").show()

  import org.apache.commons.io.FileUtils

  FileUtils.deleteDirectory(
    Paths.get("/Users/sonikagautami/My_NonBackup/GITHUB/SampleSparkStructuredStream/spark-warehouse/")
      .toFile)

  df.cache()
  df.persist(StorageLevel.MEMORY_AND_DISK_SER)
  spark.catalog.cacheTable("testtableb1")

  df.write
    .partitionBy("str")
    .mode(SaveMode.Append)
    .save("file:///Users/sonikagautami/My_NonBackup/GITHUB/SampleSparkStructuredStream/src/main/resources/op")

  df.write
    .partitionBy("str")
    .mode(SaveMode.Append)
    .saveAsTable("table1")

  //dummy to keep spark-ui up for longer time
  spark.range(0, Long.MaxValue)
    .map(_.toString)
    .repartition(3)
    .count()
}