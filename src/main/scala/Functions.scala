import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}

object Functions extends App {

  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val df = spark.sparkContext.parallelize(Seq(
    ("2022-10-07", "2022-10-22 01:01:00", "01-01-2022")
  )).toDF("c1", "c2", "c3")

  import org.apache.spark.sql.functions._

  df.select(
    to_date('c1) as "c11",
    to_timestamp('c2) as "c12",
    to_date('c3, "dd-MM-yyyy") as "c13"
  )
    .select(
      year('c11),
      quarter('c11),
      minute('c12),
      second('c12),
      unix_timestamp('c12),
      from_unixtime(unix_timestamp('c12)),
      unix_timestamp(),
      current_date(),
      dayofweek('c12),
      datediff('c12, 'c12),
      months_between('c12, 'c12),
      date_add('c12, 1),
      next_day('c12, "Mon"),
      last_day('c12),
      date_format('c12, "yyyy-MM-dd'T'HH:mm:ss")
    )
    .show()

  val df2 = spark.sparkContext.parallelize(Seq(
    ("You're impossible", "Abcdef", " xyz  ")
  )).toDF("s1", "s2", "s3")

  df2.select(
    trim('s3),
    ltrim('s3),
    rtrim('s3),
    lower('s1),
    upper('s1),
    initcap('s1),
    reverse('s1),
    lpad('s2, 10, "-"),
    rpad('s2, 10, "."),
    length('s2),
    concat('s1, 's2, 's3, lit("added")),
    concat_ws("#", 's1, 's2, 's3, lit("added")),
    regexp_extract('s1, "\\s[a-z]+", 0),
    regexp_replace('s1, "\\s[a-z]+", "Nothing"),
    translate('s1, "im", ""),
  )
    .show()

  val df3 = spark.sparkContext.parallelize(Seq(
    (1.0, 1.11123, 1.17, 1)
  )).toDF("c1", "c2", "c3", "c4")

  df3.select(
    round('c1),
    round('c2, 2),
    round('c3, 1),
    round('c4, 3)
  )
    .show()

  val df4 = spark.sparkContext.parallelize(Seq(
    (Array(1, 2, 3, 4, 5), Array("one", "two", "three"))
  )).toDF("c1", "c2")

  df4.select(
    array_min('c1),
    array_max('c1),
    array_contains('c1, 2),
    array_sort('c2),
    array_except('c2, 'c2),
    array_union('c2, 'c2),
    array_distinct(array_union('c2, 'c2)),
    array_join('c2, "#")
  ).show()

  val df5 = spark.sparkContext.parallelize(Seq(
    """
      |{
      | "key": "value",
      | "arr": [1, 2, 3]
      |}
      |""".stripMargin.replaceAll("\\n", "")
  )).toDF("c1")

  val schema = StructType(
    Seq(
      StructField("key", DataTypes.StringType, true),
      StructField("arr", ArrayType(DataTypes.IntegerType), true),
    ))
  df5.show(truncate = 200, numRows = 10)

  df5.select(
    from_json('c1, schema),
    get_json_object('c1, "/"),
    to_json(from_json('c1, schema)),
  ).show(truncate = 200, numRows = 10)

  df5.select(
    from_json('c1, schema) as "c11"
  ).select(
    'c11.getItem("key"),
    'c11.getField("arr"),
    'c11.getItem("arr").getItem(0)
  ).show(truncate = 200, numRows = 10)

  val df6 = spark.sparkContext.parallelize(
    0 until 100
  ).toDF("c1")

  df6.repartition(4)
    .select(
      monotonically_increasing_id(),
      spark_partition_id(),
      'c1
    ).show(40, 200)

  df6.select(
    'c1,
    when('c1 <= 10, "0-10")
      when('c1 <= 20, "10-20")
      when('c1 <= 30, "20-30")
      otherwise ("> 30") as "c2",
    when('c1 <= 50, "0-50")
      when('c1 <= 100, "50-100") as "c3"
  )
    .show()

  val df7 = spark.sparkContext.parallelize(Seq[Tuple3[String, String, Boolean]](
    ("1", null: String, true),
    (null: String, "hi", true),
    (null: String, null: String, true),
    ("1", null: String, false),
  )).toDF("c1", "c2", "c3")

  df7.select(
    coalesce('c1, lit("default"))
  ).show()


  def udfF1(a: Int): String = if (a < 35) "Fail" else "Pass"

  val f1 = udf(udfF1(_: Int): String)

  df6.select(
    'c1,
    f1('c1)
  ).show(50)

  df6.createOrReplaceTempView("t1")
  spark.sqlContext.udf.register("f1", udfF1(_: Int): String)
  spark.sql(
    """
      |select *, f1(c1) from t1
      |""".stripMargin
  ).show()
}
