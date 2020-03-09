package com.huangbing.spark.sql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object FavTeacherSQLWithDSL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("favTeacher")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //DataSet也是分布式数据集，是对RDD的进一步封装，是更智能的RDD
    //DataSet就是只包含一行的DataFrame
    val lines: Dataset[String] = spark.read.textFile("C:\\yarnData\\favt\\input")

    val subjectAndTeacher: Dataset[(String, String)] = lines.map(line => {
      val arr: Array[String] = line.split("[/]")
      val arrSub: Array[String] = arr(2).split("[.]")
      val subject: String = arrSub(0)
      val teacher: String = arr(3)
      (subject, teacher)
    })

    //DataFrame其实就是DataSet[Row]
    val df: DataFrame = subjectAndTeacher.toDF("subject","teacher")

    import org.apache.spark.sql.functions._

    val res: DataFrame = df
      .groupBy($"subject", $"teacher")
      .agg(count("*") as "cnts")
      .select($"subject", $"teacher",$"cnts", row_number() over (Window.partitionBy($"subject") orderBy ($"cnts" desc)) as "rk")
      .select("*").where($"rk" <= 2)

    res.show()

    spark.stop()


  }

}
