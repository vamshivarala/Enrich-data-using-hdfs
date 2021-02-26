package ca.mcit.bigdata.hdfs

case class TripRoute(trip: Trip,route :Route)

case class EnrichedTrip(trip: Trip, route: Route, calendarDates: CalendarDates)
object EnrichedTrip {
  def toCSV(output: EnrichedTrip): String = {
    s"${output.trip.tripId}," +
      s"${output.trip.serviceId}," +
      s"${output.trip.routeId}," +
      s"${output.trip.tripHeadSign}," +
      s"${output.trip.wheelchairAccessible}," +
      s"${output.calendarDates.date}," +
      s"${output.calendarDates.exceptionType}," +
      s"${output.route.routeLongName}," +
      s"${output.route.routeColor}\n"
  }
}

