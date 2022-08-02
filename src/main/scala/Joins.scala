import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object Joins extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("TestJoins")
    .getOrCreate()

  val s1 = Seq(
    ("a", 1, "t0"),
    ("b", 2, "t2"),
    ("c", 3, "t3"),
    ("d", 4, "t4"),
    ("e", 5, "t5"))
  val s2 = Seq(
    ("aa", 11, "t1"),
    ("bb", 22, "t2"),
    ("cc", 33, "t3"),
    ("dd", 44, "t4"),
    ("ee", 55, "t6"))

  import spark.sqlContext.implicits._

  val df1 = spark.sparkContext.parallelize(s1).toDF("str", "num", "category")
  val df2 = spark.sparkContext.parallelize(s2).toDF("str2", "num2", "category2")

  df1.join(df2).where('category === 'category2)
    .show()

  df1.join(df2, joinExprs = 'category === 'category2, joinType = "inner")
    .show()

  val joinExprs = df1.col("category") === df2.col("category2")
  df1.join(df2, joinExprs, joinType = "inner")
    .show()

  df1.createOrReplaceTempView("tb1")
  df2.createOrReplaceTempView("tb2")
  spark.sql(
    """
      |select * from tb1 join tb2 on category == category2
      |""".stripMargin)
    .show()

  val df3 = df2.withColumnRenamed("category2", "category")
  val df4 = df1.join(df3).where(df1.col("category") === df3.col("category"))
  df4.show()
  df4.select(df1.col("category"), df3.col("category")).show()
  df1.join(df3, "category").show()

  df1.join(df2, 'category === 'category2, joinType = "left_outer")
    .show()
  df1.join(df2, 'category === 'category2, joinType = "right_outer")
    .show()
  df1.join(df2, 'category === 'category2, joinType = "outer")
    .show()

  df1.join(df2, 'category === 'category2, joinType = "left_anti")
    .show()
  df1.join(df2, 'category === 'category2, joinType = "left_semi")
    .show()

  df1.crossJoin(df2)
    .show()

  
  df1.join(broadcast(df2), 'category === 'category2)
    .explain()
  spark.sql(
    """
      |select
      |/*+ MAPJOIN(tb2) */
      |* from tb1 JOIN tb2
      |on tb1.category == tb2.category2
      |""".stripMargin)
    .explain()
}
