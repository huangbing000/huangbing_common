package com.huangbing.sparksteaming.wordcount

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWithState {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountDemo")
    val ssc = new StreamingContext(conf,Seconds(10))
    //设置检查点
    ssc.checkpoint("C:\\spark\\checkpoints")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest", //latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("sparkwc")

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics, kafkaParams)
    )

    //存储kafka偏移量
    stream.foreachRDD(kafkaRDD => {

      if(!kafkaRDD.isEmpty()){
        //取出kafka偏移量
        val offsetRanges: Array[OffsetRange] = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        //打印kafka偏移量
        println("================kafka偏移量打印=======================")
        offsetRanges.foreach(x => {
          println(s"kafkapartition=${x.partition}  kafkapartitionoffsets=${x.fromOffset}")
        })
        println("======================================================")

        //手动更新kafka偏移量
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    val lines: DStream[String] = stream.map(_.value())
    val wordAndOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))

    val func: (Iterator[(String,Seq[Int],Option[Int])]) => Iterator[(String,Int)] = (it) => {
      val arr = new collection.mutable.ArrayBuffer[Int]()
      it.map(x => (x._1,x._2.sum + x._3.getOrElse(0)))
    }

    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    reduced.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
