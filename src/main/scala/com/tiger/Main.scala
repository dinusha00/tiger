package com.tiger

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

object Main {

  val APP = "tiger"

  def main(args: Array[String]): Unit = {
    println(s"calling $APP main v3")

    val dataPath = args(0)
    println(s"dataPath:$dataPath")

    val keyToSearch = args(1)
    println(s"keyToSearch:$keyToSearch")

    val spark = SparkSession.builder().appName(APP).getOrCreate()

    import spark.implicits._
    val ds = spark.read.textFile(dataPath).select(input_file_name, $"value").as[(String, String)].rdd
    ds.foreachPartition(partition => {
      partition.foreach(t => {
        val file = t._1
        val content = t._2
        if (content != null && content.indexOf(keyToSearch) > 0) {
          println("#########################################################")
          println(content)
          println(file)
          println("#########################################################")
        }
      })
    }
    )

    spark.stop()
    println(s"end $APP main")
  }
}
