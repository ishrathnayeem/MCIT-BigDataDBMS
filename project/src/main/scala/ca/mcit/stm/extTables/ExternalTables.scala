package ca.mcit.stm.extTables

import Main

trait ExternalTables {
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

  println("ext_trips TABLE was CREATED\n")

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

  println("ext_calendar_dates TABLE was CREATED\n")

  stmt execute
    """CREATE EXTERNAL TABLE fall2019_ishrath.ext_frequencies (
      |trip_id        STRING,
      |start_time     TIMESTAMP,
      |end_time       TIMESTAMP,
      |headway_secs   INT
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/fall2019/ishrath/project4/frequencies'
      |TBLPROPERTIES (
      |"skip.header.line.count" = "1",
      |"serialization.null.format" = "")""".stripMargin

  println("ext_frequencies TABLE was CREATED\n")

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
      |start_time	          TIMESTAMP,
      |end_time	            TIMESTAMP,
      |headway_secs        	INT,
      |date	                INT,
      |exception_type      	INT
      |)
      |PARTITIONED BY (wheelchair_accessible int)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS PARQUET""".stripMargin

  println("enriched_trip TABLE was CREATED\n")
}
