import org.apache.spark.sql.types.LongType

object Functions2 extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val df = spark.sqlContext.sparkContext.parallelize(
    Seq(1, 2, 3).zip(Seq("a", "b", "c"))
  ).toDF("c1", "c2")

  df.printSchema()

  import org.apache.spark.sql.functions._

  df.select(
    'c1 leq 10,
    pow('c1 * 5, lit(2)) + 1,
    'c2.isNotNull,
    'c2.endsWith("c"),
    expr("c1 is not null"),
    expr("c1 * 2 + 5"),
    expr("pow(c1, c1) + 5"),
    not('c1.lt(10)),
    'c1.cast(LongType),
    'c1.cast("long"),
  ).show()

  df.where('c1.lt(10).and('c2.contains("a")))
    .show()
  df.where('c1.lt(2).or('c2.equalTo("c")))
    .show()

  df.stat.crosstab("c1", "c2")
    .show()

  df.repartition(2)
    .withColumn("partition-id", spark_partition_id())
    .selectExpr("*")
    .show()
  df.repartition('c1)
    .withColumn("partition-id", spark_partition_id())
    .selectExpr("*")
    .show()
  df.repartition(2, 'c1)
    .withColumn("partition-id", spark_partition_id())
    .selectExpr("*")
    .show()
  df.repartition(3).coalesce(2)
    .withColumn("partition-id", spark_partition_id())
    .selectExpr("*")
    .show()

  df.select(
    struct('c1, 'c2) as "struct1"
  ).show()
  df.selectExpr(
    "struct(c1, c2) as struct1",
    "(c1, c2) as struct2",
  ).show()

  df.select(
    split(lit("a b c"), " ") as "arr"
  ).selectExpr("*")
    .show()
  df.select(
    split(lit("a b c"), " ") as "arr"
  ).select('arr(0), explode('arr))
    .show()

  df.select(
    map('c1, 'c2)
  ).show()

  df.selectExpr("*",
    """
      |'{"key": 1, "val": true}' as jsonStr
      |""".stripMargin)
    .select(get_json_object('jsonStr, "$.key") as "c11")
    .show(truncate = false)

  df.union(df)
    .groupBy('c1)
    .agg(
      "c2" -> "count",
      "c2" -> "collect_list",
    )
    .show()
}
