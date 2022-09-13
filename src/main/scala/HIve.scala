import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HIve extends App {
  val spark = SparkSession
    .builder()
    .master("local")
    .config("hive.metastore.uris", "hive_metastore_uris")
    .config("spark.sql.warehouse.dir", "warehouseLocation")
    .enableHiveSupport()
    .getOrCreate()

  import spark.sql

  sql("use testdb")
  var a = sql("msck repair table external-table")

  import spark.implicits._

  spark
    .read
    .option("basePath", "/hive/test.db/table1")
    .orc("/hive/test.db/table1/partition-col1=202208*",
      "/hive/test.db/table1/partition-col1=20220901")
    .groupBy('col1)
    .agg(countDistinct('user))
    .collect
}
