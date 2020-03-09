package com.huangbing.spark.favteacher

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object FavTeacher3 {

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

    val subjects: Array[String] = subjectAndTeacher.map(_._1._1).distinct().collect()

    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(new SubjectPartitioner(subjects),_+_)

    import MySortRules.teacherOrdering
    val res = reduced.mapPartitions(it => {
      val treeSet: mutable.TreeSet[((String, String), Int)] = new mutable.TreeSet[((String, String), Int)]()
      it.foreach(x => {
        treeSet.add(x)
        if (treeSet.size == 3) {
          val last: ((String, String), Int) = treeSet.last
          treeSet.remove(last)
        }
      })
      treeSet.toIterator
    })

    res.saveAsTextFile("C:\\yarnData\\favt\\output")

  }

}

class SubjectPartitioner(subjects: Array[String]) extends Partitioner {

  var partitionRules: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  var i = 0;
  for(subject <- subjects) {
    partitionRules.put(subject,i)
    i += 1
  }

  override def numPartitions: Int = subjects.length

  override def getPartition(key: Any): Int = {
    val tuple = key.asInstanceOf[(String,String)]
    partitionRules(tuple._1)
  }
}
