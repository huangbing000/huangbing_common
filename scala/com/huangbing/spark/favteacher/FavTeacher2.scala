package com.huangbing.spark.favteacher

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FavTeacher2 {

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

    //把reduced存储在spark集群的executor中，以备多次触发job用
    reduced.persist()
    //reduced.cache()

    //得到所有学科，触发一次job
    val subjects: Array[String] = reduced.map(x => x._1._1).distinct().collect()

    //总共有多少学科，就触发多少job
    for(subject <- subjects) {
      //得到只包含一个学科得rdd
      val filtered = reduced.filter(_._1._1.equals(subject))
      val sorted: RDD[((String, String), Int)] = filtered.sortBy(_._2,false)
      val res = sorted.map(x => (x._1._2,x._2))
      res.saveAsTextFile("C:\\yarnData\\favt\\output\\"+subject+"out")
    }

    sc.stop()

  }

}
