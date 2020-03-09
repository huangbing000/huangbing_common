package com.huangbing.hiveonspark

import org.apache.spark.sql.{DataFrame, SparkSession}
object StaticDayActivity {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")

    val spark = SparkSession.builder()
      .appName("staticActivity")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("create database 1705e")

    spark.sql("create table 1705e.t_web_log(ip string,uid string,access_time string,url string) partition by (day string) row format delimited fields terminated by ','")

    spark.sql("load data inpath '/weblog/2017-09-15.log' into table 1705e.t_web_log partition(day='2017-09-15')")
    spark.sql("load data inpath '/weblog/2017-09-16.log' into table 1705e.t_web_log partition(day='2017-09-16')")
    spark.sql("load data inpath '/weblog/2017-09-17.log' into table 1705e.t_web_log partition(day='2017-09-17')")

    //创建日活跃用户表
    spark.sql("create table 1705e.t_user_active_day(ip string,uid string,access_time string,url string) partitioned by(day string) row format delimited fields terminated by ','")

    //统计9-15的日活跃用户
//    val v_tmp = spark.sql("select ip,uid,access_time,url,row_number() over(partition by uid order by access_time desc) rk from 1705e.t_web_log where day = '2017-09-15'")
//    v_tmp.createTempView("v_tmp")
//
//    val v_tmp1 = spark.sql("select ip,uid,access_time,url from v_tmp where rk = 1")
//    v_tmp1.createTempView("v_tmp1")
//    spark.sql("insert into 1705e.t_user_active_day partition(day='2017-09-15') select * from v_tmp1")

    spark.sql("insert into 1705e.t_user_active_day partition(day='2017-09-15') select * from (select ip,uid,access_time,url from (select ip,uid,access_time,url,row_number() over(partition by uid order by access_time desc) rk from 1705e.t_web_log where day = '2017-09-15') tmp where rk = 1) tmp2")

    //创建日新用户表
    spark.sql("create table 1705e.t_user_new_day like 1705e.t_user_active_day")

    //创建历史用户表
    spark.sql("create table 1705e.t_user_history(uid string)")

    //统计9-15日新增用户
    spark.sql("insert into 1705e.t_user_new_day partition(day='2017-09-15') select tuad.ip ip,tuad.uid uid,tuad.access_time access_time,tuad.url url from 1705e.t_user_active_day tuad left join 1705e.t_user_history tuh on tuad.uid = tuh.uid where tuad.day = '2017-09-15' and tuh.uid is NULL")

    //更新9-15日历史用户的数据
    spark.sql("insert into 1705e.t_user_history select uid from 1705e.t_user_new_day where day = '2017-09-15'")

    spark.stop()

  }
}
