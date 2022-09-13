import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object Jbbc extends App {

  val expected_max_partitions = 1000
  val maxAutoIncrementIdValue = 100012 //max-id of table
  var partitions: Long = (maxAutoIncrementIdValue / expected_max_partitions) + 1;
  partitions = if (partitions > expected_max_partitions) expected_max_partitions else partitions

  val connectionProperties = new Properties()
  connectionProperties.put("user", "db_user")
  connectionProperties.put("password", "db_pwd")
  connectionProperties.put("driver", "driverName")
  connectionProperties.put("fetchsize", "jdbc_option_fetchsize")
  connectionProperties.put("partitionColumn", "jdbc_option_partitionColumn")
  connectionProperties.put("lowerBound", "0")
  connectionProperties.put("upperBound", maxAutoIncrementIdValue.toString)
  connectionProperties.put("numPartitions", partitions.toString)


  val pgDataDF = SparkSession.builder()
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.ui.port", "4051")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "hdfs://tmp/events")
    .config("spark.hadoop.fs.s3a.endpoint", "s3a://tmp/data")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


    .getOrCreate()
    .read
    .jdbc("dbUrl", "tableName", connectionProperties)

  pgDataDF.write.format("parquet")
    .option("compression", "outputFileCompression")
    .partitionBy("timecol", "hash")
    .mode(SaveMode.Append)
    .save("op-path")
}
