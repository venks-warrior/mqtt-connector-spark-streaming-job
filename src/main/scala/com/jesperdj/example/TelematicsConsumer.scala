package com.jesperdj.example

import java.util

import com.fasterxml.jackson.databind.deser.std.StringDeserializer
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer, ByteArrayDeserializer}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.apache.spark.streaming.mqtt.MQTTUtils

object TelematicsConsumer {

  def main(args: Array[String]) {

      if (args.length < 2) {
        System.err.println(
          "Usage: MQTTWordCount <MqttbrokerUrl> <topic>")
        System.exit(1)
      }

      val sparkConf = new SparkConf().setAppName("MQTTWordCount").set("spark.driver.allowMultipleContexts", "true")
      val sparkContext = new SparkContext(sparkConf)
      val ssc = new StreamingContext(sparkContext, Seconds(20))

      val lines = MQTTUtils.createStream(ssc, "tcp://localhost:1883", "test", StorageLevel.MEMORY_ONLY_SER_2)
      val words = lines.flatMap(x => x.toString.split(" "))
      val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
      print("******** printing wordCount")
      wordCounts.print()
      val kafkaOpTopic = "kafka-spark"
      val kafkaBrokers = "localhost:9092" // comma separated list of broker:host can be given
    try {
      lines.foreachRDD(rdd => {
        System.out.println("# RDD events received = " + rdd.count() + " no. of RDD partitions = " + rdd.getNumPartitions)

        rdd.foreachPartition(partition => {
          // Print statements in this section are shown in the executor's stdout logs
          //print("#records size in partition "+partition.size +partition.seq)
          val props = new util.HashMap[String, Object]()
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

          val kafkaProducer = new KafkaProducer[String, String](props)
          partition.foreach(record => {
            val data = record.toString
            // As as debugging technique, users can write to DBFS to verify that records are being written out
            // dbutils.fs.put("/tmp/test_kafka_output",data,true)
            val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
            println("producing messages to kafka on topic" + s" ${kafkaOpTopic}, brokers" + s" ${kafkaBrokers}")
            kafkaProducer.send(message)
          })
          kafkaProducer.close()
        })

      })

      ssc.start()
      ssc.awaitTermination()

    } catch {
      case e: Exception => print("Exception occurred while running job " + e.printStackTrace())
    }finally {
    }
  }

}
