import java.sql.{Connection, DriverManager, ResultSet}

import ca.mcit.stm.configuration.Config
 
object Main extends App with Config{
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/fall2019_ishrath;user=ishrathnayeem;password=Faheemnayeem1.")
  val stmt = connection.createStatement()

  stmt execute("CREATE EXTERNAL TABLE fall2019_ishrath.ext_trips (" +
    "route_id    INT," +
    "service_id  STRING," +
    "trip_id STRING," +
    "trip_headsign   STRING," +
    "direction_id    STRING," +
    "shape_id    STRING," +
    "wheelchair_accessible   STRING," +
    "note_fr STRING," +
    "note_en STRING )" +
    "ROW FORMAT DELIMITED" +
    "FIELDS TERMINATED BY ','" +
    "STORED AS TEXTFILE" +
    "LOCATION '/user/fall2019/ishrath/project4/trips'" +
    "TBLPROPERTIES (\n\"skip.header.line.count\" = \"1\",\n\"serialization.null.format\" = \"\")")


  stmt execute("CREATE EXTERNAL TABLE fall2019_ishrath.ext_calendar_dates ("+
    "service_id  STRING,"+
    "date    INT,"+
    "exception_type  INT )"+
  "ROW FORMAT DELIMITED"+
  "FIELDS TERMINATED BY ','"+
  "STORED AS TEXTFILE"+
  "LOCATION '/user/fall2019/ishrath/project4/calendar_dates'"+
  "TBLPROPERTIES (\n    \"skip.header.line.count\" = \"1\",\n    \"serialization.null.format\" = \"\")")


  stmt execute ("SET hive.exec.dynamic.partition=true")
  stmt execute ("SET hive.exec.dynamic.partition.mode=nonstrict")

  stmt execute("CREATE TABLE fall2019_ishrath.enriched_trip ("+
    "route_id                STRING,"+
    "service_id	            STRING,"+
    "trip_id	                STRING,"+
    "trip_headsign       	STRING,"+
    "direction_id        	INT,"+
    "shape_id	            INT,"+
    "note_fr             	STRING,"+
    "note_en             	STRING,"+
    "start_time	            STRING,"+
    "end_time	            STRING,"+
    "headway_secs        	INT,"+
    "date	                INT,"+
    "exception_type      	INT )"+
  "PARTITIONED BY (wheelchair_accessible int)"+
  "ROW FORMAT DELIMITED"+
  "FIELDS TERMINATED BY ','"+
  "STORED AS PARQUET")

  stmt executeQuery ("INSERT OVERWRITE TABLE fall2019_ishrath.enriched_trip PARTITION (wheelchair_accessible)"+
  "SELECT t.route_id,t.service_id,t.trip_id,t.trip_headsign,t.direction_id,t.shape_id,t.note_fr,t.note_en,"+
    "f.start_time,f.end_time,f.headway_secs,c.date,c.exception_type,t.wheelchair_accessible"+
  "FROM ext_trips t"+
  "FULL OUTER JOIN ext_frequencies f"+
  "ON t.trip_id = f.trip_id"+
  "FULL OUTER JOIN ext_calendar_dates c"+
  "ON t.service_id = c.service_id")


  val res: ResultSet = stmt.executeQuery("SELECT * FROM logsqs")

 // println()

  while (res.next()) {
    println("mid"+res.getString(2))

  }

  stmt.close()
  connection.close()
}
