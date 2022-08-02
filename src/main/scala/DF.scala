import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object DF extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("test1")
    //.enableHiveSupport()
    .config(SQLConf.COMPRESS_CACHED.key, true)
    .config(SQLConf.GROUP_BY_ALIASES.key, true)
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(0 to 9)

  import spark.sqlContext.implicits._

  val df = rdd.toDF("col1")
  df.printSchema()
  df.show(10)

  val df2 = spark.sparkContext.parallelize(100 to 109).toDF("col1")

  import org.apache.spark.sql.functions._

  df.select("col1")
  df.select(column("col1"))
  df.select(col("col1"))
  df.select($"col1")
  df.select('col1)
  df.select('col1, 'col1 > 10).show(10)
  df.select('col1, ('col1 > 10).as("col2")).show(10)
  df.select('col1, ('col1 > 10).alias("col2")).show(10)
  df.select('col1, 'col1 * 10 - 'col1).show(10)

  df.selectExpr("*")
  df.selectExpr("*", "((col1 * 10) - col1) as newCol").show(10)

  df.select(count('col1)).show(10)
  df.selectExpr("count(col1)").show(10)

  df.map(r => r.getInt(0))
  df.mapPartitions(seq => seq.map(_.getInt(0)))

  df.select('col1, 'col1 > lit(5) as "col2")
  df.select('col1, 'col1 > typedlit[Int](5) as "col2")
    .withColumnRenamed("col2", "col2-name-updated")
    .withColumn("newcol1", spark_partition_id())

  df.select('col1, 'col1 > 5 as "col2")
    .groupBy('col2)
    .agg(collect_list('col1) as "collist")
    .show()

  df.union(df2)
  df.unionAll(df2)
  df.unionByName(df2)
  df.unionByName(df2, allowMissingColumns = true)
    .select('col1, 'col1 + 10 as "col2", 'col1 - 10 as "col3")
    .sort(
      desc("col1"),
      desc_nulls_last("col2"),
      asc_nulls_first("col3")
    ).show()

  //handling bad-data => df.na
  df.na.drop(how = "any") //remove rows if any column has null value
  df.na.drop(how = "all") //remove rows if all columns have null values
  df.na.drop(minNonNulls = 2)
  df.na.drop(cols = List("col1"))
  df.na.fill(1) // null|NaN numerics
    .na.fill("empty") //null strings

  df.na.replace("col1", Map(1 -> 200)).show()

  val inputDir = "file:///Users/sonikagautami/My_NonBackup/GITHUB/SampleSparkStructuredStream/src/main/resources/inputDir"
  spark.read
    .option("header", false)
    .format("csv")
    .load(inputDir)
    .select(spark_partition_id(), input_file_name())
    .show(10, 200)

  spark.read
    .option("header", false)
    .csv(inputDir)
}
