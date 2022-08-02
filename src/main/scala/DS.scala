import org.apache.spark.sql.SparkSession

object DS extends App {

  val seq = Seq("one", "two", "three", "four").map(s => TestC(s, s.length))

  val spark = SparkSession.builder()
    .master("local")
    .appName("TestDs")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val ds = spark.sparkContext.parallelize(seq).toDS
  ds.printSchema()

  spark.sparkContext.parallelize(seq).toDF().as[TestC]
    .printSchema()
  spark.createDataset(seq)
    .printSchema()

  ds.filter(_.value < 5)
    .map(_.str.length)
    .show()


}

case class TestC(str: String, value: Int)
