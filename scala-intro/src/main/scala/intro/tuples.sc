val person = ("Dursun", "KOC", 'M', 34)
person.productArity
person.getClass
person.productIterator.foreach(println)
person.productPrefix
person.productElement(0)
person.productElement(1)
person.productElement(2)
person.productElement(3)

def greet(name: String, gender: Char) =
  gender match {
    case 'M' => s"Hello Mr.${name}"
    case 'F' => s"Hello Mrs.${name}"
    case _ => s"Hello ${name}"
  }
greet(person._1, person._3)