package io.jacob.sec

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * Created by xiaoy on 2018/2/9.
  */
object QuoteStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "aisvr2:6667")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "aisvr1:2181")
    properties.setProperty("group.id", "test1")
    properties.setProperty("auto.offset.reset", "earliest")

    val myConsumer = new FlinkKafkaConsumer010[String]("mktdt00-quote", new SimpleStringSchema(), properties)

    myConsumer.assignTimestampsAndWatermarks(new SSEQuoteTmGenerator)

    val sseQuoteStream = env.addSource[String](myConsumer)

    sseQuoteStream.map( x => {
      val fields = x.split("\\|")

      SSEQuote(fields(1), // securityId
        fields(6).toFloat, //openPrice
        fields(7).toFloat, //highPrice
        fields(8).toFloat, //lowPrice
        fields(9).toFloat, //tradePrice
        fields(11).toFloat, //buyPrice1
        fields(12).toLong, //buyVolume1
        fields(13).toFloat, //sellPrice1
        fields(14).toLong //sellVolume1
      )
    }).keyBy("securityId").timeWindow(Time.seconds(30)).max("tradePrice").print()


    env.execute("SSE Quote Stream")
  }

}

case class SSEQuote (
                    securityId: String,
                    openPrice:Float,
                    highPrice: Float,
                    lowPrice: Float,
                    tradePrice: Float,
                    buyPrice1: Float,
                    buyVolume1: Long,
                    sellPrice1: Float,
                    sellVolume1: Long
                    )

class SSEQuoteTmGenerator extends AssignerWithPeriodicWatermarks[String] {

  val maxOutOfOrderness = 2000L // 3.5 seconds

  var currentMaxTimestamp: Long = 0
  val timeFormat = new SimpleDateFormat("HH:mm:ss.SSS")

  override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
    val timestamp = timeFormat.parse(element.split("\\|").last).getTime
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    timestamp
  }

  override def getCurrentWatermark: Watermark = {
    // return the watermark as current highest timestamp minus the out-of-orderness bound
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }
}

class SZSEQuoteTmGenerator extends AssignerWithPeriodicWatermarks[String] {

  val maxOutOfOrderness = 2000L // 3.5 seconds

  var currentMaxTimestamp: Long = 0
  val timeFormat = new SimpleDateFormat("HHmmss")

  override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
    val timestamp = timeFormat.parse(element.split("\\|").last).getTime
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    timestamp
  }

  override def getCurrentWatermark: Watermark = {
    // return the watermark as current highest timestamp minus the out-of-orderness bound
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }
}