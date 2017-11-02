package drunkedcat.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @author whitelilis@gmail.com
  */
object Streaming extends Serializable {
  @transient lazy val log = LogManager.getRootLogger

  val topics = "test"
  val brokers = "wx-kafka-03:9092,wx-kafka-04:9092,wx-kafka-05:9092,wx-kafka-06:9092"
  val groupId = "wizard_1"
  val intervalSeconds = 5


  def doWhat(stream: InputDStream[(String, String)] ) = {
    stream.map{
      keyAndMessage => {
        val k = keyAndMessage._2.substring(2)
      }
    }.print()
  }

  // -------------------------  skeleton below ------------------------
  def functionToCreateContext(): StreamingContext = {
    val sparkConf = new SparkConf()
    val envHome = System.getenv("HOME")
    if(envHome.contains("/Users")) {
      // mac os, for test
      sparkConf.setMaster("local")
    }else{
      // set by user from cmd line
    }
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
      .setAppName("Streaming")

    val ssc = new StreamingContext(sparkConf, Seconds(intervalSeconds))

    // Create direct kafka stream with brokers and topics
    val topicsSet = "test".split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest",
      "group.id" -> groupId
    )
    val km = new KafkaManager(kafkaParams)
    val kafkaDirectStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    log.warn(s"Initial Done***>>>")

    //---------------------- do things here ------------------------------

    doWhat(kafkaDirectStream)

    //---------------------- end do things, update zookeeper's offset ------------------------------
    kafkaDirectStream.foreachRDD(rdd => {
      if (!rdd.isEmpty)
        km.updateZKOffsets(rdd)
    })

    ssc
  }

  def main(args: Array[String]) {
    val ssc = functionToCreateContext()
    ssc.start()
    ssc.awaitTermination()
  }
}
