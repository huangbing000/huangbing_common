package com.huangbing.spark.customsort

object StuSortrRule {

  implicit val sortRule = new Ordering[(String,Int,Int)] {
    override def compare(x: (String, Int, Int), y: (String, Int, Int)): Int = {
      if(x._2 - y._2 == 0) {
        y._3 - x._3
      } else {
        x._2 - y._2
      }
    }
  }

}
