package com.huangbing.spark.customsort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CustomerSort2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CustomerSort1")
    val sc = new SparkContext(conf)

    val text: RDD[String] = sc.parallelize(Array("zhangchaobing 22 80","jiayiju 20 85","zhangrunze 20 95","yanglongfei 25 77"))

    val maped: RDD[(String, Int, Int)] = text.map(line => {
      val fields: Array[String] = line.split(" ")
      (fields(0), fields(1).toInt, fields(2).toInt)
    })

    import StuSortrRule.sortRule
    val sorted: RDD[(String, Int, Int)] = maped.sortBy(x => x)

    println(sorted.collect().toBuffer)

    sc.stop()

  }

}
