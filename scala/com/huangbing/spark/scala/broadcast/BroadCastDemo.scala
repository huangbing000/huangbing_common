package com.huangbing.spark.scala.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("BroadCastDemo")
    val sc = new SparkContext(conf)

   val names: RDD[String] = sc.makeRDD(Array("tom","jack","lili","hank","marry","lee"),4)

    //获取分区数
    println(names.getNumPartitions)

    val psMap: Map[String, String] = Map("prefix" -> "hello","suffix" -> "eat shit")

    val bValue: Broadcast[Map[String, String]] = sc.broadcast(psMap)

    val maped: RDD[String] = names.map(x => {
      val map = bValue.value
      map("prefix") + " " + x + " " + map("suffix")
    })

    maped.foreach(println(_))

    sc.stop()
  }

}
