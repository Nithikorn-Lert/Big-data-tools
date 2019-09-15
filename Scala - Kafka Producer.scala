package Kafka_Scala

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization._
import org.apache.kafka.common._
import org.apache.kafka._
// Ref: Manish Kumar, Chanchal Singh - Building Data Streaming Applications with Apache Kafka
//　before run this file, you must change file name to My_kafka_producer(.scala)
object My_kafka_producer {
  // Unit = void in java and C
  def main(args: Array[String]): Unit ={
    val producer_props = new Properties()
    val SS = "org.apache.kafka.common.serialization.StringSerializer"
    producer_props.put("bootstrap.servers", "127.0.0.1:9092")
    // put( KEY, VALUE ) => add information to Properties we create

    producer_props.put("key.serializer", SS)
    producer_props.put("value.serializer", SS)
    // serialize KEY, VALUE

    producer_props.put("acks", "all")
    // response mode of acknowledgement from broker;
    // acks:all => P. will only receive ack. when leader has received ack. for all replicas

//    producer_props.put("retires", new Integer(1))
    // amount of waiting time for resending messages when sending fails.

    producer_props.put("batch.size", new Integer(1000))

    producer_props.put("linger.ms", new Integer(1))
    // amount of waiting time for additional messages before sending a current batch

    // producer_props.put("buffer.memory", "__"); default 32MB

    val producer_ishere = new KafkaProducer[String, String](producer_props)
    val sending_data = new ProducerRecord[String, String]("test", "Hi! Kafka Scala")

    producer_ishere.send(sending_data)
    producer_ishere.close()

  }
}
