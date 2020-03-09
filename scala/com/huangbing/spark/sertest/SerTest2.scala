package com.huangbing.spark.sertest

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerTest2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SerTest2")
    val sc = new SparkContext(conf)

    val datas: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6),4)
    println(datas.getNumPartitions)

    //rules在Driver端初始化，伴随着Task发送到Driver端，每个Task有一个单独的实例
    val rules = new Rules

    val tuples = datas.map(num => {
      val bookName: String = rules.ruleMap.getOrElse(num, "未知")
      //executor的name
      val addressName: String = InetAddress.getLocalHost.getHostAddress
      //task的Name
      val threadName = Thread.currentThread().getName
      (bookName, addressName,threadName,rules.toString)
    }).collect()

    tuples.foreach(println(_))

    sc.stop()





  }

}
