package ca.mcit.stm.fileManaging

import java.io.FileNotFoundException

import ca.mcit.stm.configuration.Config
import org.apache.hadoop.fs.Path

trait FileManaging extends Config{
  if (hadoop
    .exists(new Path("/user/fall2019/ishrath/project4")))

    try {
      hadoop
        .delete(new Path("/user/fall2019/ishrath/project4"), true)
      hadoop
        .listStatus(new Path("/user/fall2019/ishrath/project4"))
      println("FAILED!! to DELETE PROJECT4 Directory")
    }
    catch {
      case f: FileNotFoundException =>
        println("PROJECT4 Directory was DELETED!\n")
    }

  try {
    hadoop
      .mkdirs(new Path("/user/fall2019/ishrath/project4"))
    hadoop
      .listStatus(new Path("/user/fall2019/ishrath/project4"))
    println("PROJECT4 Directory was CREATED\n")
  }
  catch {
    case f: FileNotFoundException =>
      println("FAILED!! to CREATE PROJECT4 Directory\n")
  }

  try {
    hadoop
      .mkdirs(new Path("/user/fall2019/ishrath/project4/trips"))
    hadoop
      .listStatus(new Path("/user/fall2019/ishrath/project4/calendar_dates"))
    println("TRIPS folder was CREATED\n")
  }

  catch {
    case f: FileNotFoundException =>
      println("TRIPS folder was CREATED\n")
  }

  try {
    hadoop
      .mkdirs(new Path("/user/fall2019/ishrath/project4/calendar_dates"))
    hadoop
      .listStatus(new Path("/user/fall2019/ishrath/project4/calendar_dates"))
    println("CALENDAR folder was CREATED\n")
  }
  catch {
    case f: FileNotFoundException =>
      println("FAILED!! to create CALENDAR folder\n")
  }
  try {
    hadoop
      .mkdirs(new Path("/user/fall2019/ishrath/project4/frequencies"))
    hadoop
      .listStatus(new Path("/user/fall2019/ishrath/project4/frequencies"))
    println("FREQUENCIES folder was CREATED\n")
  }
  catch {
    case f: FileNotFoundException =>
      println("FAILED!! to create FREQUENCIES folder\n")
  }
}
