import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row, TypedColumn}

object udaf extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val df = spark.sparkContext.parallelize((1 until (10))).toDF

  val udaf_spark30: TypedColumn[Row, Int] = min_custom.toColumn.name("min_custom")

  df.select(udaf_spark30).show()


}

object min_custom extends Aggregator[Row, Int, Int] {
  override def zero: Int = Int.MaxValue

  override def reduce(b: Int, a: Row): Int = if (b < a.getInt(0)) b else a.getInt(0)

  override def merge(b1: Int, b2: Int): Int = if (b1 < b2) b1 else b2

  override def finish(reduction: Int): Int = reduction

  override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}
