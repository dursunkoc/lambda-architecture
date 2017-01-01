val weekDays = "Mon" :: "Tue" :: "Wed" :: "Thr" :: "Fri" :: Nil
val weekEnd = List("Sat", "Sun")
val days = weekDays ::: weekEnd
weekDays ++ weekEnd
List(weekDays, weekEnd)
List(weekDays, weekEnd).flatten
val indicies = (1 to 7).toList
val daysWithIndicies = days zip indicies
weekDays.head
weekDays.tail
weekDays.size
weekDays.reverse
weekDays.contains("Mon")
weekDays.productArity
weekDays(0)
weekDays(1)