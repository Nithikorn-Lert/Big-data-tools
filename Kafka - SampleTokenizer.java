/*  Ref: B. Bejeck, Kafka Streams in Action: Real-time apps and microservices with the Kafka Streams API
you need to change file name to "SampleTokenizer.java" before run the following code.
 */

import kafka.consumer.KafkaStream;
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
import scala.collection.immutable.Stream;

import java.util.Arrays;
import java.util.Properties;


public class SampleTokenizer {

    private static final Logger LOG = LoggerFactory.getLogger(SampleTokenizer.class);
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_yelling_app_ID");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // setting
        StreamsConfig streamConf = new StreamsConfig(props);
        Serde<String> stringSerde = Serdes.String();
    // stream starter waiting for initialization
        StreamsBuilder builder = new StreamsBuilder();

        //       <K>   , <V>
        KStream<String, String> StreamStarter = builder.stream(
                "src-topic", Consumed.with(stringSerde, stringSerde));

        KStream<String, String> tokenizer = StreamStarter.flatMapValues(
                string -> Arrays.asList(string.toLowerCase().split("\\W+")));
            tokenizer.to("out-topic", Produced.with(stringSerde, stringSerde));
            tokenizer.print(Printed.<String, String> toSysOut().withLabel("SampleTokenizer_result"));

    // set the configulation to stream starter
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamConf);

        LOG.info("starting");
        kafkaStreams.start();
        Thread.sleep(99999999);
        LOG.info("preparing to exit");
        kafkaStreams.close();
    }
}
