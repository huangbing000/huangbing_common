package com.huangbing.spark.favteacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("FavTeacher1")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("C:\\yarnData\\favt\\input")

    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val arr: Array[String] = line.split("[/]")
      val arrSub: Array[String] = arr(2).split("[.]")
      val subject: String = arrSub(0)
      val teacher: String = arr(3)
      ((subject, teacher), 1)
    })

    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)

    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    val res: RDD[(String, List[(String, Int)])] = grouped.mapValues(it => {
      val list: List[((String, String), Int)] = it.toList
      val sorted: List[((String, String), Int)] = list.sortBy(_._2).reverse
      val maped: List[(String, Int)] = sorted.map(x => {
        (x._1._2, x._2)
      })
      maped
    })
    res.saveAsTextFile("C:\\yarnData\\favt\\output")

    sc.stop()

  }

}
