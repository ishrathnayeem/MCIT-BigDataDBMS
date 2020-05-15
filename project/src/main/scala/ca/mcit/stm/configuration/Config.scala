package ca.mcit.stm.configuration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait Config {

  val conf = new Configuration()
  val uri = "/user/fall2019/ishrath"
  conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))

  val hadoop: FileSystem = FileSystem.get(conf)
 // println(hadoop.getUri)

}
