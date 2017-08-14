package me.wonwoo

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}


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
    def catalog =
    s"""{
       |"table":{"namespace":"default", "name":"table1"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"int"},
       |"col2":{"cf":"cf2", "col":"col2", "type":"boolean"}
       |}
       |}""".stripMargin


    //TODO where data save ?
    val map = stream.map(record => (record.key, record.value))

    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("1")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("2")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("3")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("4")))),
      (Bytes.toBytes("5"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("5"))))
    ))

    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      TableName.valueOf(tableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) =>
          put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      })

    ssc.start
    ssc.awaitTermination
  }

  case class Record(col0: Int, col1: Int, col2: Boolean)

  //    sc.parallelize(map).toDF.write.options(
  //      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
  //    .format(“org.apache.spark.sql.execution.datasources.hbase”)
  //    .save()
}
