package com.huangbing.spark.sql

import com.huangbing.spark.ipstatic.IpUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object IpStaticWithUdf {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("IpStaticWithUdf")
      .getOrCreate()

    import spark.implicits._
    val accessLogDataSet: Dataset[String] = spark.read.textFile("C:\\yarnData\\ip\\input")
    val accessDF: DataFrame = accessLogDataSet.map(line => {
      val arr = line.split("[|]")
      val ip = arr(1)
      val ipNum: Long = IpUtil.ip2Long(ip)
      ipNum
    }).toDF("ip")

    val ruleDataSet: Dataset[String] = spark.read.textFile("C:\\yarnData\\ip\\rule")
    val ruleRes: Dataset[(Long, Long, String)] = ruleDataSet.map(line => {
      val arr = line.split("[|]")
      (arr(2).toLong, arr(3).toLong, arr(6))
    })

    //收集到Driver端
    val rules: Array[(Long, Long, String)] = ruleRes.collect()
    //广播规则
    val rulesRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rules)


    val ip2Provincef:(Long) => String = (ip) => IpUtil.searchIp(rulesRef.value,ip)

    //自定义UDF，要发送到各个executor中执行，所以udf中用的rule，要用广播变量给广播出去
    spark.udf.register("ip2Province",ip2Provincef)

    //sql风格
//    accessDF.createTempView("v_access")
//    val v_tmp1: DataFrame = spark.sql("select ip2Province(ip) as province from v_access")
//    v_tmp1.createTempView("v_tmp1")
//    val res: DataFrame = spark.sql("select province,count(*) as cnts from v_tmp1 group by province order by cnts desc")

    //DSL风格
    import org.apache.spark.sql.functions._
    val res:DataFrame = accessDF.select(callUDF("ip2Province", $"ip") as "province")
      .groupBy($"province").agg(count("*") as "cnts")
      .orderBy($"cnts" desc)
    res.show()
    spark.stop()
  }

}
