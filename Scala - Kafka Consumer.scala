import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger
import java.util._

// Ref: Manish Kumar, Chanchal Singh - Building Data Streaming Applications with Apache Kafka
//　before run this file, you must change file name to My_kafka_consumer(.scala)

object My_kafka_consumer {
  private val log = Logger.getLogger(getClass)

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val topic_name = "test" // select "test" as a topic
    val topic_list = new ArrayList[String]
    topic_list.add(topic_name) // for sequential message

    val consumer_prop = new Properties /*config option of consumer*/
    consumer_prop.put("bootstrap.servers", "127.0.0.1:9092")
    consumer_prop.put("group.id", "Stock_1001") // name of fetching file. to tell kafka which message we want to fetch
    consumer_prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumer_prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumer_prop.put("enable.auto.commit", "true")
    consumer_prop.put("auto.commit.interval.ms", "500") /* time for committing*/

    val consumer_ishere = new KafkaConsumer[String, String](consumer_prop)
    consumer_ishere.subscribe(topic_list)

    log.info(topic_name + " are already subscribed !!")
    val i = 0

    try
        while ( {
          true
        }) { // while there are some messages from broker
          val records = consumer_ishere.poll(10)
          import scala.collection.JavaConversions._
          for (record <- records) { // for record in records --Python
            log.info("value: :" + record.value)
          }
          //Map topic and data together
          consumer_ishere.commitAsync(new OffsetCommitCallback() {
            override def onComplete(map: Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit = {
              // onComplete() will fire when the task is completed, even if it failed.
            }
          })
        }
    catch {
      case exc: Exception =>

      // while no any message
    } finally try
      consumer_ishere.commitSync()
    finally consumer_ishere.close()
  }
}
