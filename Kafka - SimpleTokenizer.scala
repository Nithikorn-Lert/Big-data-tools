/*
Ref: B. Bejeck, Kafka Streams in Action: Real-time apps and microservices with the Kafka Streams API
WARNING: Don't use KStream for Java API in Scala. I spent half a day for troubleshooting
you need to change file name to "SimpleTokenizer.scala" before run the following code.

General Kafka stream program structure
1. Define the setting
2. Crate Serializable/DeSerializable instance via Serde
3. Build processor topology
4. Create and Start Kafka stream (KStream + KafkaStreams)

                                  Processor
-------------      /--------------------------------------\      -------------
| src-topic |---> | source + tokenization function + sink  |---> | out-topic |
-------------      \--------------------------------------/      -------------
 */

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, KTable, Produced}
import org.apache.kafka.streams.scala._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util._
import org.apache.kafka.streams.kstream.Printed // main API


object SimpleTokenizer {

  private val LOG = LoggerFactory.getLogger(SimpleTokenizer.getClass)
  def main(args: Array[String]): Unit = {

    val props = new Properties
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "SimpleTokenizer")
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")

// 1. Define the setting
    val streamConf = new StreamsConfig(props)

// 2. Crate Serializable/DeSerializable instance via Serde
    val stringSerde =  Serdes.String()

// 3. Build processor topology
    val builder : StreamsBuilder = new StreamsBuilder

// 4. Create and Start Kafka stream (KStream + KafkaStreams)
    // StreamStarter will contain streamed text
    val StreamStarter : KStream[String, String]= builder.stream[String, String]("src-topic", Consumed.`with`[String, String](stringSerde, stringSerde))
    val tokenizer = StreamStarter.flatMapValues((s:String) => s.toLowerCase().split("\\W+"))
      //       .to( topic )( implicit produced )
      tokenizer.to("out-topic")(Produced.`with`[String, String](stringSerde, stringSerde))
      tokenizer.print(Printed.toSysOut().withLabel("SampleTokenizer_result"))

    val kafkaStreams : KafkaStreams = new KafkaStreams(builder.build(), streamConf)

    kafkaStreams.start()
    LOG.info("starting")
    Thread.sleep(99999999)
    LOG.info("preparing to exit")
    kafkaStreams.close()

  }
}
