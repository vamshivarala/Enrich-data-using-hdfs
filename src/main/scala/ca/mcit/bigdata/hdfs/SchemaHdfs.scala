package ca.mcit.bigdata.hdfs

case class Trip(tripId: String,
                serviceId: String,
                routeId: Int,
                tripHeadSign: String,
                wheelchairAccessible: Boolean)
object Trip {
  def apply(in: String): Trip = {
    val fields: Array[String] = in.split(",", -1)
    Trip(fields(2), fields(1), fields(0).toInt, fields(3), if (fields(6) == "1") true else false)
  }
}

case class Route(routeId: Int,
                 routeLongName: String,
                 routeColor: String)
object Route {
  def apply(in: String): Route = {
    val fields: Array[String] = in.split(",", -1)
    Route(fields(0).toInt, fields(3), fields(6))
  }
}
case class CalendarDates(serviceId: String,
                         date: String,
                         exceptionType: Int)
object CalendarDates {
  def apply(in: String): CalendarDates = {
    val fields: Array[String] = in.split(",", -1)
    CalendarDates(fields(0), fields(1), fields(2).toInt)
  }
}



