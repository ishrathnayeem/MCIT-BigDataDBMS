package ca.mcit.stm.filesUploading

import java.io.FileNotFoundException

import ca.mcit.stm.configuration.Config
import org.apache.hadoop.fs.Path

class FileUpload extends Config{
  try {
    hadoop
      .listStatus(new Path("/user/fall2019/ishrath/stm"))
      .foreach(println)
    println("I found my File")
  }
  catch{
    case f : FileNotFoundException =>
      println("file not found")
  }
}
