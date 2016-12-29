package intro

object HelloScala extends App {
  implicit class Person(val name: String) {
    private val firstLast = name.split(" ");
    var first: String = null
    var last: String = null
    if (firstLast.length >= 1) {
      first = firstLast(0)
    }
    if (firstLast.length >= 2) {
      last = firstLast(1)
    }
    override def toString() = this.first + " " + this.last
  }

  val p = new Person("Dursun KOC")
  implicit val k:Person = "Yasemin KOC"
  
  println(k.first)
  println(p.first)
  
  def greet(implicit p:Person)=s"Hello $p"
  println(greet)
  println(greet(p))
}