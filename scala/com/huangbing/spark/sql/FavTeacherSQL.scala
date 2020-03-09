package com.huangbing.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FavTeacherSQL {

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

    //注册临时视图
    df.createTempView("t_sub_teacher")

    //使用sql进行batch的统计了

    val tem_v1: DataFrame = spark.sql("select subject,teacher,count(*) as cnts from t_sub_teacher group by subject,teacher")
    tem_v1.createTempView("tem_v1")

    val tem_v2: DataFrame = spark.sql("select subject,teacher,cnts,row_number() over(partition by subject order by cnts desc) as rk from tem_v1")
    tem_v2.createTempView("tem_v2")

    val result: DataFrame = spark.sql("select * from tem_v2 where rk <= 2")
    result.cache()
    result.show()
    result.write.json("C:\\yarnData\\favt\\output\\json")

    spark.stop()



  }

}
