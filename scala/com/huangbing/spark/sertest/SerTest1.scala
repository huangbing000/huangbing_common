package com.huangbing.spark.sertest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerTest1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SerTest1")
    val sc = new SparkContext(conf)

    val datas: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6),4)
    println(datas.getNumPartitions)

    val tuples = datas.map(num => {
      val rules = new Rules
      val bookName: String = rules.ruleMap.getOrElse(num, "未知")
      (bookName, rules.toString)
    }).collect()

    tuples.foreach(println(_))

    sc.stop()





  }

}
