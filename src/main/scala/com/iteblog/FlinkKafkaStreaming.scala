package com.iteblog

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * Created by https://www.iteblog.com on 2016/5/3.
  */
object FlinkKafkaStreaming {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "aisvr2:6667")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "aisvr1:2181")
    properties.setProperty("group.id", "iteblog")


    val stream = env.addSource(new FlinkKafkaConsumer010[String]("mktdt00-quote",
      new SimpleStringSchema(), properties))
//    stream.setParallelism(4).writeAsText("hdfs:///tmp/iteblog/data")
    stream.print()

    env.execute("IteblogFlinkKafkaStreaming")
  }
}