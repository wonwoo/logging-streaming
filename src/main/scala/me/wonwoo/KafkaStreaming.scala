package me.wonwoo

import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD

import scala.util.Properties


object KafkaStreaming {

  val tableName = "logs"
  val columnFamily = "test"

  def main(args: Array[String]): Unit = {
    //    System.setProperty("hadoop.home.dir", "hadoop-2.7.1")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[KafkaJsonDeserializer],
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
    val conf = HBaseConfiguration.create

    val ZOOKEEPER_QUORUM = "127.0.0.1"
    conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)

    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))
    map.foreachRDD((value: RDD[(String, String)], time: Time) => {
      var put = new Put(Bytes.toBytes("row1"))
      put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("test_column_name"), Bytes.toBytes("test_value"))
      table.put(put)
    })
    ssc.start
    ssc.awaitTermination
  }


  //    sc.parallelize(map).toDF.write.options(
  //      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
  //    .format(“org.apache.spark.sql.execution.datasources.hbase”)
  //    .save()
}
