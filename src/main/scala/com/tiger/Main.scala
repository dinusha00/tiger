package com.tiger

import java.io.{File, FileInputStream, FileOutputStream}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

import scala.io.Source

object Main {

  val MSG_TYPE = "New"

  def main(args: Array[String]): Unit = {
    println(s"calling main")

    val p = getClass.getPackage
    val name = p.getImplementationTitle
    val version = p.getImplementationVersion
    println(s"app:$name version:${version}")

    if (args.length != 3) {
      throw new RuntimeException("number of arguments not correct. dataPath, keysFilePath and resultsFolderPath should be provided !!!")
    }

    val dataPath = args(0)
    println(s"dataPath:$dataPath")

    val keysFilePath = args(1)
    println(s"keysFilePath:$keysFilePath")

    val resultsFolderPath = args(2)
    println(s"resultsFolderPath:$resultsFolderPath")

    val keys = getKeys(keysFilePath)
    println(s"keys.size:${keys.size}")

    val spark = SparkSession.builder().appName(name).getOrCreate()
    val successMatches = spark.sparkContext.longAccumulator("success matches")

    import spark.implicits._
    val ds = spark.read.textFile(dataPath).select(input_file_name, $"value").as[(String, String)].rdd
    ds.foreachPartition(partition => {
      partition.foreach(t => {
        val file = t._1
        val content = t._2
        if (content != null && content.size > 74) {
          val keyToMatch = content.substring(content.size - 74, content.size - 31)
          if (keys.contains(keyToMatch)) {
            println("--------------------------------------------------------")
            successMatches.add(1)
            val keyMatched = content.trim
            val filePath = file.trim
            println(s"key matched:${keyMatched.substring(0, 36)}")
            println(keyMatched)
            println(filePath)
            copyFile(filePath, resultsFolderPath)
            println("#########################################################")
          }
        }
      })
    }
    )

    spark.stop()
    println(s"successMatches:${successMatches.value}")
    println(s"end main")
  }

  def getKeys(filename: String): Seq[String] = {
    convertKeys(readFile(filename))
  }

  def readFile(filename: String): Seq[String] = {
    val bufferedSource = Source.fromFile(filename)
    try {
      val lines = (for (line <- bufferedSource.getLines()) yield line).toList
      lines
    } finally {
      bufferedSource.close
    }
  }

  def convertKeys(keys: Seq[String]): Seq[String] = {
    val convertedKeys = new Array[String](keys.size)
    for (i <- 0 until keys.length) {
      convertedKeys(i) = s"${keys(i)}, $MSG_TYPE @"
    }
    convertedKeys.toSeq
  }

  def copyFile(src: String, dest: String): Unit = {
    val file = new File(src.replaceAll("file:", ""))
    new FileOutputStream(s"$dest/${file.getName}") getChannel() transferFrom(new FileInputStream(file.getPath) getChannel, 0, Long.MaxValue)
  }
}
