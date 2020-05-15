import java.sql.{Connection, DriverManager, ResultSet}

import ca.mcit.stm.configuration.Config
 
object Main extends App with Config{
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/fall2019_ishrath;user=ishrathnayeem;password=Faheemnayeem1.")
  val stmt = connection.createStatement()

  stmt.execute("CREATE EXTERNAL TABLE fall2019_ishrath.logsqs ("+    "version INT,"+    "url string)"+  "row format DELIMITED fields terminated by ','"+  "LOCATION '/user/fall2019/ishrath/encoding/' ")


  val res: ResultSet = stmt.executeQuery("SELECT * FROM logsqs")

 // println()

  while (res.next()) {
    println("mid"+res.getString(2))

  }

  stmt.close()
  connection.close()
}
