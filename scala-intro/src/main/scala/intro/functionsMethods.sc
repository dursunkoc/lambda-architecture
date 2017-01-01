

def compareStringASC(a: String, b: String): Int =
  (a, b) match {
    case _ if a > b => 1
    case _ if b > a => -1
    case _ => 0
  }

def compareStringDESC(a: String, b: String): Int =
  (a, b) match {
    case _ if a > b => -1
    case _ if b > a => 1
    case _ => 0
  }

val comASC = compareStringASC(_, _)
val comDESC: (String, String) => Int = compareStringDESC

def compareSmart(compareFnc:(String,String)=>Int)(a:String,b:String) = compareFnc(a,b)

val getCompareFunc = (reversed:Boolean) => if(reversed) comASC else comDESC

def compareSmartPA(reversed: Boolean) = compareSmart(getCompareFunc(reversed))(_,_)

val comp1 = compareSmartPA(true)
comp1("abc","xyz")
