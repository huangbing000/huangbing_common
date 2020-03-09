package com.huangbing.hiveonspark

import org.apache.spark.sql.{DataFrame, SparkSession}

object SelStudent {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val spark = SparkSession.builder()
      .appName("staticstu")
      .master("local[*]")
      .enableHiveSupport() //启用hive支持
      .getOrCreate()

    val res: DataFrame = spark.sql("select sex,avr(age) ageavg from `1705e`.`t_student` group by sex order by ageavg")

    res.show()

    spark.stop()
  }
}
