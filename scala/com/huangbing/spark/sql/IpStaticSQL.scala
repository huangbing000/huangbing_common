package com.huangbing.spark.sql

import com.huangbing.spark.ipstatic.IpUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object IpStaticSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("IpStatic").getOrCreate()

    import spark.implicits._
    //DataSet是只有一列的DataFrame
    val lines: Dataset[String] = spark.read.textFile("C:\\yarnData\\ip\\rule")

    val toDf: Dataset[Rule] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      Rule(fields(2).toLong, fields(3).toLong, fields(6))
    })

    val ruleDf: DataFrame = toDf.toDF()

    val accessLines: Dataset[String] = spark.read.textFile("C:\\yarnData\\ip\\input")
    val accessDf: DataFrame = accessLines.map(line => {
      val arr: Array[String] = line.split("[|]")
      val ip: String = arr(1)
      val ipNum: Long = IpUtil.ip2Long(ip)
      (ipNum)
    }).toDF("ip")


    //sql方式
//    ruleDf.createTempView("v_rule")
//    accessDf.createTempView("v_access")
//    val v_tmp_1: DataFrame = spark.sql("select va.ip as ip,vr.province as province from v_access as va left join v_rule as vr on (va.ip >= vr.ipStart and va.ip <= vr.ipEnd)")
//    v_tmp_1.createTempView("v_tmp_1")
//    val res: DataFrame = spark.sql("select province,count(*) as cnts from v_tmp_1 group by province order by cnts desc")

    //DSL方式
    import org.apache.spark.sql.functions._
    val res: DataFrame = accessDf.join(ruleDf, $"ip" >= $"ipStart" and $"ip" <= $"ipEnd", "left_outer")
      .groupBy($"province").agg(count("*") as "cnts")
      .orderBy($"cnts" desc)

    res.show()
    res.explain(true)

    spark.stop()
  }

}


case class Rule(ipStart: Long,ipEnd: Long, province: String)
