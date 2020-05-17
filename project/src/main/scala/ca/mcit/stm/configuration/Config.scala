package ca.mcit.stm.configuration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
trait Config {

  val conf = new Configuration()
  conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop/etc/cloudera/hdfs-site.xml"))
  val hadoop: FileSystem = FileSystem.get(conf)

}
