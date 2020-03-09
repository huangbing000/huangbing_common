package com.huangbing.spark.customsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomerSort1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CustomerSort1")
    val sc = new SparkContext(conf)

    val text: RDD[String] = sc.parallelize(Array("zhangchaobing 22 80","jiayiju 20 85","zhangrunze 20 95","yanglongfei 25 77"))
    val stus: RDD[Student] = text.map(line => {
      val fields: Array[String] = line.split(" ")
      new Student(fields(0), fields(1).toInt, fields(2).toInt)
    })

    val sorted: RDD[Student] = stus.sortBy(stu => stu)
    println(sorted.collect().toBuffer)

    sc.stop()
  }

}

class Student(val name: String, val age: Int, val score: Int) extends Ordered[Student] with Serializable {
  override def compare(that: Student): Int = {
    if(that.age - this.age == 0) {
      //如果年龄相等，按分数倒序排
      that.score - this.score
    } else {
      //如果不相等，按年龄正序排
      this.age - that.age
    }
  }

  override def toString: String = s"$name  $age   $score  ###"
}
