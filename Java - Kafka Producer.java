import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
// Ref: Manish Kumar, Chanchal Singh - Building Data Streaming Applications with Apache Kafka
// before run this file, you must change file name to My_kafka_producer(.java)
// P. = Producer, ack. = acknowledge
public class My_kafka_producer {
    public static void main (final String[] args) {
        String SS = "org.apache.kafka.common.serialization.StringSerializer";
        Properties producer_props = new
                Properties();
        // new Properties()	=> Create object Properties

        //                                         broker:port
        producer_props.put("bootstrap.servers", "127.0.0.1:9092");
        // put( KEY, VALUE ) => add information to Properties we create

        producer_props.put("key.serializer", SS);
        producer_props.put("value.serializer", SS);
        // serialize KEY, VALUE

        producer_props.put("acks", "all");
        // response mode of acknowledgement from broker;
        // acks:all => P. will only receive ack. when leader has received ack. for all replicas

        producer_props.put("retires", 1);
        // amount of waiting time for resending messages when sending fails.

        producer_props.put("batch.size", 1000);

        producer_props.put("linger.ms", 1);
        // amount of waiting time for additional messages before sending a current batch

        // producer_props.put("buffer.memory", "__"); default 32MB

        KafkaProducer<String, String> producer_ishere = new
                KafkaProducer<String, String> (producer_props);

        ProducerRecord sending_data = new
                ProducerRecord<String, String>("test", "Hi! Kafka Java");

        producer_ishere.send(sending_data);

        producer_ishere.close();

    }

}