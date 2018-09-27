package com.tiger

import org.apache.spark.sql.SparkSession

object Main {

  val APP = "tiger"

  def main(args: Array[String]): Unit = {
    println(s"calling $APP main")

    val dataPath = args(0)
    println(s"dataPath:$dataPath")

    val keyToSearch = args(1)
    println(s"keyToSearch:$keyToSearch")

    val spark = SparkSession.builder().appName(APP).getOrCreate()

    val rdd = spark.sparkContext.wholeTextFiles(dataPath)
    rdd.foreachPartition(partition => {
      partition.foreach(t => {
        val name = t._1
        val content = t._2
        if (content.indexOf(keyToSearch) > 0) {
          println("#########################################################")
          println(name)
          println("#########################################################")
        }
      })
    }
    )

    spark.stop()
    println(s"end $APP main")
  }
}
