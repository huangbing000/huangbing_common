package com.huangbing.spark.sertest

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerTest4 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SerTest4")
    val sc = new SparkContext(conf)

    val datas: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6),4)
    println(datas.getNumPartitions)

    val tuples = datas.map(num => {
      //rules在Executor中初始化，并且只有一个实例，多个Task共享
      val bookName: String = ExecutorStaticRules.ruleMap.getOrElse(num, "未知")
      //executor的name
      val addressName: String = InetAddress.getLocalHost.getHostAddress
      //task的Name
      val threadName = Thread.currentThread().getName
      (bookName, addressName,threadName,ExecutorStaticRules.toString)
    }).collect()

    tuples.foreach(println(_))

    sc.stop()





  }

}
