package algo

object HouseLamp extends App {

  val houses = Seq(0, 1, 2, 3, 4, 6, 8, 9, 10)
  val lamps = Seq(5, 7)

  def maxRadiousOf(houses: Seq[Int],
                   lamps: Seq[Int]): Int = {

    var max = 0;
    var housesCount = 0;

    var indexOfL = 0;
    for (i <- 0 until (houses.length + lamps.length)) {

      if (indexOfL < lamps.length && lamps(indexOfL) == i) {
        if (max < housesCount) {
          max = housesCount
        }
        housesCount = 0
        indexOfL += 1
      } else {
        housesCount += 1
      }

    }

    if (max < housesCount) {
      max = housesCount
    }

    max
  }

  println(maxRadiousOf(houses, lamps))
}
