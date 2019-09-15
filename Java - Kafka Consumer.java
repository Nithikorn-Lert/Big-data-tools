import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger; // log4j is a tool to help the programmer output log statements to a variety of output targets
import java.util.*;

// Ref: Manish Kumar, Chanchal Singh - Building Data Streaming Applications with Apache Kafka
//　before run this file, you must change file name to My_kafka_consumer(.scala)
public class My_kafka_consumer {

    private static final Logger log = Logger.getLogger(My_kafka_consumer.class);

    public static void main(String[] args) throws Exception {

        String topic_name = "test"; // select "test" as a topic
        List<String> topic_list = new ArrayList<String>();
        topic_list.add(topic_name); // for sequential message

        Properties consumer_prop = new Properties(); /*config option of consumer*/    consumer_prop
                    .put("bootstrap.servers", "127.0.0.1:9092");                      consumer_prop
                    .put("group.id", "Stock_1001");                                   consumer_prop
                // name of fetching file. to tell kafka which message we want to fetch
                    .put("key.deserializer",
                        "org.apache.kafka.common.serialization.StringDeserializer");  consumer_prop
                    .put("value.deserializer",
                         "org.apache.kafka.common.serialization.StringDeserializer"); consumer_prop
                    .put("enable.auto.commit", "true");                               consumer_prop
                    .put("auto.commit.interval.ms", "500"); /* time for committing*/

        KafkaConsumer<String, String> consumer_ishere = new KafkaConsumer<String, String>(consumer_prop);
        consumer_ishere.subscribe(topic_list);

        log.info(topic_name + " are already subscribed !!" );
        int i = 0;

        try {
            while (true) {// while there are some messages from broker

                ConsumerRecords<String, String> records = consumer_ishere.poll(10);

                for (ConsumerRecord record : records) // for record in records --Python
                    log.info("value: :" + record.value());

                //Map topic and data together
                consumer_ishere.commitAsync(new OffsetCommitCallback() {
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                        // onComplete() will fire when the task is completed, even if it failed.
                    }
                });
            }
        } catch (Exception exc) { // while no any message
        }

        finally {
            try {
                consumer_ishere.commitSync();
            } finally {
                consumer_ishere.close();
            }
        }
    }
}
