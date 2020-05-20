import java.sql.{Connection, DriverManager}

import ca.mcit.stm.configuration.Config
import ca.mcit.stm.fileManaging.FileManaging
import org.apache.hadoop.fs.Path
 
object Main extends App with Config with FileManaging{
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/fall2019_ishrath;user=ishrathnayeem;password=Faheemnayeem1.")
  val stmt = connection.createStatement()

  try {
    hadoop
      .copyFromLocalFile(new Path("/Users/ishrathnayeem/IntelliJ IDEA/mcit/gtfs_stm/trips.txt"),
        new Path("/user/fall2019/ishrath/project4/trips/trips.txt"))
  }
  try {
    hadoop
      .copyFromLocalFile(new Path("/Users/ishrathnayeem/IntelliJ IDEA/mcit/gtfs_stm/calendar_dates.txt"),
        new Path("/user/fall2019/ishrath/project4/calendar_dates/calendar_dates.txt"))
  }
  try {
    hadoop
      .copyFromLocalFile(new Path("/Users/ishrathnayeem/IntelliJ IDEA/mcit/gtfs_stm/frequencies.txt"),
        new Path("/user/fall2019/ishrath/project4/frequencies/frequencies.txt"))
  }

  println("DATA LOADED\n\n")
  println("!!! ENRICHMENT IN PROGRESS !!!\n\n")

  stmt.executeUpdate(
    """INSERT OVERWRITE TABLE fall2019_ishrath.enriched_trip PARTITION (wheelchair_accessible)
      |SELECT t.route_id,t.service_id,t.trip_id,t.trip_headsign,t.direction_id,t.shape_id,
      |t.note_fr,t.note_en,f.start_time,f.end_time,f.headway_secs,c.date,c.exception_type,t.wheelchair_accessible
      |FROM ext_trips t
      |FULL OUTER JOIN fall2019_ishrath.ext_calendar_dates c
      |ON t.service_id = c.service_id
      |FULL OUTER JOIN fall2019_ishrath.ext_frequencies f
      |ON t.trip_id = f.trip_id""".stripMargin)

  println("<---ENRICHMENT DONE--->")

  stmt.close()
  connection.close()
}
