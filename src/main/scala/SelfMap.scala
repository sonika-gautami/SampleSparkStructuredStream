
object SelfMap extends App {

  val map = new Mapp[Int]
  map.set("1", 1)
  map.set("2", 2)
  map.set("22", 22)

  println(map.get("1"))
  println(map.get("2"))
  println(map.get("22"))
}

class Mapp[T] {

  import scala.collection.mutable.ListBuffer

  private[this] val hashes = new Array[ListBuffer[Tuple2[String, T]]](10)

  def get(key: String): T = {
    hashes(hashStr(key)).find(_._1.equals(key)).map(_._2).get
  }

  def set(key: String, value: T): Unit = {
    hashes(hashStr(key)) match {
      case null =>
        val lb = new ListBuffer[Tuple2[String, T]]
        hashes(hashStr(key)) = lb
        lb.append(key -> value)

      case l => l.append(key -> value)
    }
  }

  def hashStr(key: String): Int = key.length
}
