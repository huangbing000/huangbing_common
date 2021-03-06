package com.huangbing.spark.ipstatic

object IpUtil {
  //将ip地址转换成长整型的算法
  def ip2Long(ip: String): Long = {
    val arr: Array[String] = ip.split("[.]")
    var ipNum = 0L;
    arr.foreach(x => {
      ipNum = x.toLong | ipNum << 8L
    })
    ipNum
  }

  def searchIp(ipRules: Array[(Long,Long,String)], ip: Long): String ={
    var province: String = "未知"
    var low = 0;
    var high = ipRules.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if(ip >= ipRules(middle)._1 && ip <= ipRules(middle)._2) {
        //返回找到的省份
        return ipRules(middle)._3
      }
      if(ip < ipRules(middle)._1) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    province
  }

}
