package com.huangbing.spark.sertest

object StaticRules extends Serializable {
  val ruleMap = Map(
    1 -> "hadoop",
    2 -> "spark",
    3 -> "flink",
    4 -> "kafka",
    5 -> "redis",
    6 -> "java"
  )
}
