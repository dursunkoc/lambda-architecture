import scala.util.{Failure, Success, Try}

val m = Map("A" -> 1, "B" -> 2)
def getMWithNoException(k:String) =
  Try(m(k)) match {
    case Success(s) => s
    case Failure(e) => e.getMessage
  }
getMWithNoException("A")
getMWithNoException("C")
