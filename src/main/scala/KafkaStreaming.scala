import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}



object KafkaStreaming {

  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "hadoop-2.7.1")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "logging_1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val sparkConf = new SparkConf().setAppName("KafkaStreaming").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val topics = Array("logs")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

//    stream.foreachRDD { rdd =>
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd.foreachPartition { iter =>
//        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}" )
//      }
//    }

    //TODO where data save ?
    val map = stream.map(record => (record.key, record.value))
    map.print(10)
    ssc.start
    ssc.awaitTermination
  }
}
