package ca.mcit.bigdata.hdfs

import org.apache.hadoop.fs. Path

import java.io.{BufferedReader, InputStreamReader}

object project extends App with HadoopClient {
  val inputPath ="/user/bdsf2001/varala/stm"
  val fileSourceTrips =new BufferedReader(new InputStreamReader
  (fs.open(new Path(s"$inputPath/trips.txt"))))
  val tripList: List[String] = Iterator
    .continually(fileSourceTrips.readLine()).takeWhile(_ != null)
    .toList
    .tail
  val trips: List[Trip] = tripList.map(trip => Trip(trip))
  fileSourceTrips.close()

  val fileSourceRoutes = new BufferedReader(new InputStreamReader
  (fs.open(new Path(s"$inputPath/routes.txt"))))
  val routeList: List[String] = Iterator
    .continually(fileSourceRoutes.readLine()).takeWhile(_ != null)
    .toList
    .tail
  val routes: List[Route] = routeList.map(in => Route(in))
  fileSourceRoutes.close()

  val routeLookup: Map[Int, Route] =  routes.map(route => route.routeId->route).toMap

  val tripRoute= trips.map(trip =>
    if (routeLookup.contains(trip.routeId)) TripRoute(trip, routeLookup(trip.routeId))
    else TripRoute(trip,null))
    .filter(_.route != null)

  val fileSourceCalendarDates=new BufferedReader(new InputStreamReader
  (fs.open(new Path(s"$inputPath/calendar_dates.csv"))))
  val calendarDatesList: List[String] = Iterator
    .continually(fileSourceCalendarDates.readLine()).takeWhile(_ != null)
    .toList
    .tail
  val calendarDates: List[CalendarDates] =calendarDatesList.map(calendar => CalendarDates(calendar))

  val enrichedTrip :List[EnrichedTrip] =
    for {route <- tripRoute
         calendarDates <- calendarDates
         if route.trip.serviceId == calendarDates.serviceId} yield
      EnrichedTrip(route.trip,route.route,calendarDates)
  fileSourceCalendarDates.close()

  val writer = fs.create(new Path("/user/bdsf2001/varala/course3/enrichedtrips.csv"))
  writer.writeBytes("route_id,service_id,trip_id,trip_headsign,direction_id,shape_id," +
    "wheelchair_accessible,note_fr,note_en,route_long_name")
  enrichedTrip.map(line => EnrichedTrip.toCSV(line)).foreach(line => writer.writeBytes(line))

  writer.flush()
  writer.close()
}




