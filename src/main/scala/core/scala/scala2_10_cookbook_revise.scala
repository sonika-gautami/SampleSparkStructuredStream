package core.scala

import scala.annotation.switch
import scala.util.{Failure, Success, Try}

object scala2_10_cookbook_revise extends App {
  //
  val regex = "([0-9]+)(\\w+)".r
  val regex(no, str) = "1232ndjcs"
  println(no)
  println(str)

  //

  import StringExtension._

  println("bacxyz".increment)
  println("bacxyz".increment(5))

  //
  println(BigInt("1"))
  println(BigDecimal("1.123298854678"))
  println(Integer.parseInt("10", 2))
  println(Integer.parseInt("10", 8))
  println(Integer.parseInt("10", 16))
  println(0x10)
  val str1 = null.asInstanceOf[String]
  println(str1)

  //
  Try(testException) match {
    case Success(value) => println(value)
    case Failure(exception) => println(exception.getMessage)
  }

  // type ascription (upcast - cast to upper-class at compile time)
  val s = "String type-ascription to object": Object
  println(s.getClass)

  //
  val s1 = for {
    s <- Seq("Abc", "Xyz")
    c <- s
    if c.isLower
  } yield c
  println(s1)

  for {
    t2 <- Seq("abc", "xyz").zipWithIndex
  } println(t2)

  val arr = Array.ofDim[Int](2, 2)
  for {
    i <- 0 until 2
    j <- 0 until 2
  } println(arr(i)(j))

  val matcher = 1
  println {
    (matcher: @switch) match {
      case 1 => "one"
      case 0 | 2 | 3 | 4 | 5 => "one"
      case _ => "any no > five"
    }
  }

  val obj = "test obj": Object
  obj match {
    case P1("abc", n) => println(s"P1 type $n")
    case P1("abc", 1) => println(s"P1 type")
    case P2("abc", n, true) => println(s"P2 type $n")
    case p@P2("abc", _, true) => println(s"P2 type $p")
    case (_, _) => println("tuple2 type")
    case (_, b, _) => println(s"tuple3 type $b")
    case (_, b, 3) => println(s"tuple3 type $b")
    case List(1, _, _) => println("List type starting with 1; length 3")
    case List(1, _*) => println("List type starting with 1; any length")
    case List(a, _*) => println(s"List type starting with $a; any length")
    case a :: rest => println(s"List type starting with $a elem & remianing size ${rest.size}")
    case l@List(1, _*) => println(s"List type with values = $l; any length")
    case Nil => println("Empty List")
    case a: String => println("String type")
    case a: BigDecimal => println("BigDecimal type")
    case a: Array[String] => println("Array of String type")
    case l: List[_] => println("list of any type")
    case l: List[Int] => println("list of Int type") //but this won't work; as due to type erasure at run time
    /*
    non-variable type argument Int in type pattern List[Int]
    (the underlying of List[Int]) is unchecked
    since it is eliminated by erasure
     */
    case l: Map[_, Int] => println("map of any,Int type")
    /*
    non-variable type argument Int in type pattern
    scala.collection.immutable.Map[_,Int]
    (the underlying of Map[_,Int]) is unchecked
    since it is eliminated by erasure
     */
    case (_, b: String, 3) if b == "any" => println(s"tuple3 type $b")
    case _ => println("anything default")
  }

  //
  val c = new Child2("fname", "lname", 1)
  c.f = "fname-updated"
  println(c.f)
  println(c.l)
  println(c.no)

  //
  def varargsF(i: Int, s: String*) = s"i = $i; s = ${s.mkString(" ")}"

  println(varargsF(5, Seq("1", "2", "3", "4", "5"): _*))

  // partially applied functions
  val paf1: (Int, String) => String = varargsF(_, _)
  paf1(1, "1")

  //partial functions
  val pf1 = new PartialFunction[Int, Int] {
    override def isDefinedAt(input: Int): Boolean = input != 0

    override def apply(input: Int): Int = input / 2
  }
  println(pf1.isDefinedAt(0))
  println(pf1(2))

  val pf2: PartialFunction[Int, Int] = {
    case input: Int if input != 0 => input / 2
  }
  println(pf2.isDefinedAt(0))
  println(pf2(2))


  val pfNumF1: PartialFunction[Int, Int] = {
    case n: Int if n % 2 == 0 => n / 2
  }
  val pfNumF2: PartialFunction[Int, Int] = {
    case n: Int if n % 2 != 0 => (n + 1) / 2
  }
  val fullNumF = pfNumF1 orElse pfNumF2
  println(fullNumF(2))
  println(fullNumF(3))
  println(pfNumF1.orElse(pfNumF2)(2))
  println(pfNumF1.orElse(pfNumF2)(3))

  println(fullNumF.andThen(i => i * 2)(2))

  val list1 = List[Number](1, 1.0f, 1.0, 1l)
  val list2 = List[Any](1, 1.0f, 1.0, 1l, "One", true)

  list1.view.map(_.intValue() * 10)

  Map("one" -> 1, "two" -> 2) foreach {
    case (str, no) => println(s"$str : $no")
  }

  for {
    (str, i) <- Seq("one", "two").zipWithIndex
  } println(s"$i : $str")

  for {
    (str, i) <- Seq("one", "two").zip(Stream from 1)
  } println(s"$i : $str")


  //
  println(ENUM.VAL1)
  println(ENUM.VAL2)

  //
  println((1 -> 2).productIterator.toList)

  println(Seq(6, 8, 10, 1).sorted)
  println(Seq(6, 8, 10, 1).sortWith(_ > _))
  println(scala.util.Sorting.quickSort(Seq(6, 8, 10, 1).toArray))
  println(
    Seq(new Person("abc", "xyz"),
      new Person("pqr", "def"),
      new Person("abc", "def")
    ).sorted
  )

  //
  val stream = 1 #:: 2 #:: 3 #:: Stream.empty
  stream.map(_ * 10)
  stream.map(_ * 10).take(1)
  stream.map(_ * 10).take(1).foreach(println)

  Array.ofDim[Int](1, 1)
  new Array[Int](1)
}

//
case class P1(str: String, no: Int)

case class P2(str: String, no: Int, b: Boolean)

//
object StringExtension {

  // Scala doesn’t require that methods declare that exceptions can be thrown,
  // and it also doesn’t require calling methods to catch them.
  @throws(classOf[NullPointerException])
  def testException: String = null.toString

  implicit class StringExtensionImpl(s: String) {
    def increment: Int = increment(1)

    def increment(n: Int): Int = s.length + n
  }
}

//
class TestClass1(n: Int, str: String) {
  // auxiliary constructors
  def this() {
    this(0, "")
  }

  def this(n: Int) {
    this(n, "")
  }

  def this(str: String) {
    this(0, str)
  }
}

//
class Singtlton private {

}

object Singtlton {
  private lazy val INSTANCE = new Singtlton

  def apply(s: String) = INSTANCE
}

//
class Parent(var f: String, var l: String)

class Child(f: String, l: String) extends Parent(f, l)

class Child2(f: String, l: String, var no: Int) extends Parent(f, l)

//
abstract class Base(var n: String) {
  def printName = s"Name = $n"

  def rollNo: Int
}

class Derived(n: String, no: Int) extends Base(n) {
  override def rollNo: Int = no
}


//
abstract class Animal(var _type: String) {
  //not to override in derived class
  final val greeting = "Hello !"

  //to be override in devived class by val/var
  def isPet: Boolean

  //concrete method
  def printType = s"Type: ${_type}"
}

class Pet(_type: String) extends Animal(_type) {
  //override def isPet: Boolean = true
  // OR
  val isPet = true
}

class Wild(_type: String) extends Animal(_type) {
  var isPet = false
}

//
trait Trait1 {
  def hello = "Hello from Trait1"
}

trait Trait2 {
  def hello = "Hello from Trait2"
}

class C1 extends Trait1 with Trait2 {
  override def hello = {
    super[Trait1].hello
    super[Trait2].hello
  }
}

//
trait Trait11 {
  this: Trait12 =>
}

trait Trait12 {
  this: Trait13 with Trait14 =>
}

trait Trait13 {
  this: {def mustHaveThisMethod(s: String): Int} =>
}

trait Trait14 {
  this: {
    def mustHaveThisMethod1: Int
    def mustHaveThisMethod2: Int
  } =>
}

class C111 {
  def mustHaveThisMethod(s: String): Int = s.length
}

class Traits extends Trait11 with Trait12 with Trait13 with Trait14 {
  def mustHaveThisMethod(s: String): Int = 1

  def mustHaveThisMethod1: Int = 1

  def mustHaveThisMethod2: Int = 2

  val obj = new C111 with Trait13
  println(obj.mustHaveThisMethod("abc"))
}


object ENUM extends Enumeration {
  type ENUM = Value

  val VAL1, VAL2, VAL3 = Value
}

// 4x Code Generation than Enumeration (Heavier)
trait Enum

case object VAL1 extends Enum

case object VAL2 extends Enum

case object VAL3 extends Enum

class Person(val fName: String, lName: String) extends Ordered[Person] {
  override def compare(that: Person): Int = fName.compareTo(that.fName)

  override def toString: String = s"$fName $lName"
}

object AutoCloseResource {
  def useResource[Resource <: {def close(): Unit}, T](r: Resource)
                                                     (f: Resource => T): T = {
    try {
      f(r)
    } finally {
      r.close()
    }
  }
}