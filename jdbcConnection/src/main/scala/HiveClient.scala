//Activity 4
import java.sql.{Connection, DriverManager, ResultSet}

object HiveClient extends App {
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)


  val connection: Connection = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/fall2019_ishrath;user=ishrathnayeem;password=Faheemnayeem1.")
  val stmt = connection.createStatement()

  stmt.execute("DROP TABLE IF EXISTS fall2019_ishrath.enriched_movie")
  stmt.execute("DROP TABLE IF EXISTS fall2019_ishrath.enriched_movie_gz")
  stmt.execute("DROP TABLE IF EXISTS fall2019_ishrath.enriched_movie_seq")
  stmt.execute("DROP TABLE IF EXISTS fall2019_ishrath.enriched_movie_seq_gz")
  stmt.execute("CREATE TABLE fall2019_ishrath.enriched_movie \nROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE\nAS\nSELECT m.mid,m.title,m.year,m.director,r.rid,r.stars,r.ratingdate,rw.name FROM ext_movie m\nFULL OUTER JOIN ext_rating r\nON m.mid = r.mid\nFULL OUTER JOIN ext_reviewer rw\nON rw.rid = r.rid")
  stmt.execute("DESCRIBE fall2019_ishrath.enriched_movie")
  stmt.execute("CREATE TABLE enriched_movie_gz\nROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \nAS SELECT * FROM enriched_movie")
  stmt.execute("CREATE TABLE enriched_movie_seq\nROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE\nAS SELECT * FROM enriched_movie")
  stmt.execute("CREATE TABLE enriched_movie_seq_gz\nROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \nAS SELECT * FROM enriched_movie")
  val res: ResultSet = stmt.executeQuery("SELECT * FROM enriched_movie")

  println("mid","title","year","direction","ratingdate","rid","stars","name")

  while (res.next()) {
    println(""+ res.getString(1),
      ""+ res.getString(2),""+ res.getString(3),""+ res.getString(4),
      ""+ res.getString(5),""+ res.getString(6),""+ res.getString(7),
      ""+ res.getString(8))

    //    println("mid " + res.getString(1) ,
    //      "title: " + res.getString(2),"year: " + res.getString(3),"direction: " + res.getString(4),
    //      "rid: " + res.getString(5), "stars: " + res.getString(6),"ratingdate: " + res.getString(7),
    //      "name: " + res.getString(8))
  }

  stmt.close()
  connection.close()
}

