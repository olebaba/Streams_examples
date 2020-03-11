package Classes;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer{
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        String topic = "ranking_element_count";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");

        KafkaConsumer<String, Long> kafkaConsumer = new KafkaConsumer<>(props);

        kafkaConsumer.subscribe(Collections.singletonList(topic));

        while (true){
            ConsumerRecords<String, Long> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Long> record: records){
                System.out.println("Key (user_id): " + record.key() + ", Value (# of selections): " + record.value());
                //System.out.println("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
