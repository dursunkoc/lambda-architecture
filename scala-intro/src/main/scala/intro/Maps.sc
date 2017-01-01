val countriesWithCodes = Map("Turkey" -> "TR",
  "United States of America" -> "USA",
  ("Japan", "JP"))
val countries = List("Turkey", "United States of America", "Japan")
val currencies = List("Lira", "Dolar", "Yen")
val countriesWithCurrencies = (countries zip currencies).toMap
countriesWithCurrencies.foreach(p=>println(p._1+' '+p._2))