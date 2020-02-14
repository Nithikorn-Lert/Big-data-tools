/*  Ref: B. Bejeck, Kafka Streams in Action: Real-time apps and microservices with the Kafka Streams API
you need to change file name to "Decapital_Text.java" before run the following code.

General Kafka stream program structure
1. Define the setting
2. Crate Serializable/DeSerializable instance via Serde
3. Build processor topology
4. Create and Start Kafka stream (KStream + KafkaStreams)

p. = processor
                                  Processor
-------------      /--------------------------------------\      -------------
| src-topic |---> | source + tokenization function + sink  |---> | out-topic |
-------------      \--------------------------------------/      -------------
 */

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;


public class SimpleTokenizer {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTokenizer.class);
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "SimpleTokenizer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

    // 1. Define the setting
        StreamsConfig streamConf = new StreamsConfig(props);

    // 2. Crate Serializable/DeSerializable instance via Serde
        Serde<String> stringSerde = Serdes.String();

    // 3. Build processor topology
        StreamsBuilder builder = new StreamsBuilder();

    // 4. Create and Start Kafka stream (KStream + KafkaStreams)
        KStream<String, String> StreamStarter = builder.stream(
                "src-topic", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> tokenizer = StreamStarter.flatMapValues(
                string -> Arrays.asList(string.toLowerCase().split("\\W+")));
            tokenizer.to("out-topic", Produced.with(stringSerde, stringSerde));
            tokenizer.print(Printed.<String, String> toSysOut().withLabel("SampleTokenizer_result"));
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamConf);

        kafkaStreams.start();
        LOG.info("starting");
        Thread.sleep(99999999);
        LOG.info("preparing to exit");
        kafkaStreams.close();
    }
}
