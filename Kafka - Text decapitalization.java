/*  Ref: B. Bejeck, Kafka Streams in Action: Real-time apps and microservices with the Kafka Streams API
you need to change file name to "Decapital_Text.java" before run the following code.
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

import java.util.Properties;


public class Decapital_Text {

    private static final Logger LOG = LoggerFactory.getLogger(Decapital_Text.class);
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_yelling_app_ID");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // setting
        StreamsConfig streamConfig = new StreamsConfig(props);
        Serde<String> stringSerde = Serdes.String();
        Serde<Integer> intSerde = Serdes.Integer();
    // stream starter waiting for initialization
        StreamsBuilder builder = new StreamsBuilder();

        //       <K>   , <V>
        KStream<String, String> simpleFirstStream = builder.stream("src-topic",
                Consumed.with(stringSerde, stringSerde));
        KStream<String, String> upperCasedStream = simpleFirstStream.mapValues(string -> string.toLowerCase());

            upperCasedStream.to("out-topic", Produced.with(stringSerde, stringSerde));
            upperCasedStream.print(Printed.<String, String> toSysOut().withLabel("Decaptalized_text_result"));
    // set the configulation to stream starter
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamConfig);

        LOG.info("starting");
        kafkaStreams.start();
        Thread.sleep(99999999);
        LOG.info("preparing to exit");
        kafkaStreams.close();
    }
}
