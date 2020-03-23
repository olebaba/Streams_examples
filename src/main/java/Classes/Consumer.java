package Classes;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer{

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        String topic0 = "user_count", topic1 = "ranking_element_count", topic2 = "campaign_count";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   LongDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");

        KafkaConsumer<Long, Long> kafkaConsumer = new KafkaConsumer<>(props);
        KafkaConsumer<Long, Long> kafkaConsumer1 = new KafkaConsumer<>(props);
        KafkaConsumer<Long, Long> kafkaConsumer2 = new KafkaConsumer<>(props);

        //{"user_id":"2", "ranking_element_id":"324", "campaign_id":"23"}

        kafkaConsumer.subscribe(Collections.singletonList(topic0));
        kafkaConsumer1.subscribe(Collections.singletonList(topic1));
        kafkaConsumer2.subscribe(Collections.singletonList(topic2));



        while (true){
            ConsumerRecords<Long, Long> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Long, Long> current: records){
                System.out.println(topic0 + ": " + current.key() + ", Value : " + current.value().toString());
            }

            ConsumerRecords<Long, Long> records1 = kafkaConsumer1.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Long, Long> current: records1){
                System.out.println(topic1 + ": " + current.key() + ", Value : " + current.value().toString());
            }

            ConsumerRecords<Long, Long> records2 = kafkaConsumer2.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Long, Long> current: records2){
                System.out.println(topic2 + ": " + current.key() + ", Value : " + current.value().toString());
            }

        }
    }
}
