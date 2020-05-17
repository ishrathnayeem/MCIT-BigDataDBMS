import java.io.FileNotFoundException
import java.sql.{Connection, DriverManager}

import ca.mcit.stm.configuration.Config
import org.apache.hadoop.fs.Path
 
object Main extends App with Config {
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/fall2019_ishrath;user=ishrathnayeem;password=Faheemnayeem1.")
  val stmt = connection.createStatement()

  if (hadoop
    .exists(new Path("/user/fall2019/ishrath/project4")))

    try {
      hadoop
        .delete(new Path("/user/fall2019/ishrath/project4"), true)
      hadoop
        .listStatus(new Path("/user/fall2019/ishrath/project4"))
      println("Directory not deleted")
    }
    catch {
      case f: FileNotFoundException =>
        println("Directory was deleted")
    }

  try {
    hadoop
      .mkdirs(new Path("/user/fall2019/ishrath/project4"))
    hadoop
      .listStatus(new Path("/user/fall2019/ishrath/project4"))
    println("Folder Created")
  }

  try {
    hadoop
      .mkdirs(new Path("/user/fall2019/ishrath/project4/trips"))
  }

  catch {
    case f: FileNotFoundException =>
      println("Folder not created")
  }

  try {
    hadoop
      .mkdirs(new Path("/user/fall2019/ishrath/project4/calendar_dates"))
    hadoop
      .listStatus(new Path("/user/fall2019/ishrath/project4/calendar_dates"))
    println("SubFolder Created")
  }
  catch {
    case f: FileNotFoundException =>
      println("SubFolder not Created")
  }
  try {
    hadoop
      .mkdirs(new Path("/user/fall2019/ishrath/project4/frequencies"))
    hadoop
      .listStatus(new Path("/user/fall2019/ishrath/project4/frequencies"))
    println("SubFolder Created")
  }
  catch {
    case f: FileNotFoundException =>
      println("SubFolder not Created")
  }

  stmt execute """DROP TABLE IF EXISTS ext_trips"""
  stmt execute """DROP TABLE IF EXISTS ext_frequencies"""
  stmt execute """DROP TABLE IF EXISTS ext_calendar_dates"""
  stmt execute """DROP TABLE IF EXISTS enriched_trip"""
  stmt execute
    """CREATE EXTERNAL TABLE fall2019_ishrath.ext_trips (
      |route_id               INT,
      |service_id             STRING,
      |trip_id                STRING,
      |trip_headsign          STRING,
      |direction_id           STRING,
      |shape_id               STRING,
      |wheelchair_accessible  STRING,
      |note_fr                STRING,
      |note_en                STRING
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/fall2019/ishrath/project4/trips'
      |TBLPROPERTIES (
      | "skip.header.line.count" = "1",
      |"serialization.null.format" = "")""".stripMargin

  stmt execute
    """CREATE EXTERNAL TABLE fall2019_ishrath.ext_calendar_dates (
      |service_id       STRING,
      |date             INT,
      |exception_type   INT
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/fall2019/ishrath/project4/calendar_dates'
      |TBLPROPERTIES (
      |"skip.header.line.count" = "1",
      |"serialization.null.format" = "")""".stripMargin

  stmt execute
    """CREATE EXTERNAL TABLE fall2019_ishrath.ext_frequencies (
      |trip_id        STRING,
      |start_time     STRING,
      |end_time       STRING,
      |headway_secs   INT
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/fall2019/ishrath/project4/frequencies'
      |TBLPROPERTIES (
      |"skip.header.line.count" = "1",
      |"serialization.null.format" = "")""".stripMargin

  stmt execute """SET hive.exec.dynamic.partition=true"""
  stmt execute """SET hive.exec.dynamic.partition.mode=nonstrict"""

  stmt execute
    """CREATE TABLE fall2019_ishrath.enriched_trip (
      |route_id             STRING,
      |service_id	          STRING,
      |trip_id	            STRING,
      |trip_headsign       	STRING,
      |direction_id        	INT,
      |shape_id	            INT,
      |note_fr             	STRING,
      |note_en             	STRING,
      |start_time	          STRING,
      |end_time	            STRING,
      |headway_secs        	INT,
      |date	                INT,
      |exception_type      	INT
      |)
      |PARTITIONED BY (wheelchair_accessible int)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS PARQUET""".stripMargin
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

  stmt.executeUpdate(
    """INSERT OVERWRITE TABLE fall2019_ishrath.enriched_trip PARTITION (wheelchair_accessible)
      |SELECT t.route_id,t.service_id,t.trip_id,t.trip_headsign,t.direction_id,t.shape_id,
      |t.note_fr,t.note_en,f.start_time,f.end_time,f.headway_secs,c.date,c.exception_type,t.wheelchair_accessible
      |FROM ext_trips t
      |FULL OUTER JOIN ext_frequencies f
      |ON t.trip_id = f.trip_id
      |FULL OUTER JOIN ext_calendar_dates c
      |ON t.service_id = c.service_id""".stripMargin)

  stmt.close()
  connection.close()
}
