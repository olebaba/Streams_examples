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

        String topicREC = "ranking_element_count", topicUC = "userID_count";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");

        KafkaConsumer<String, Long> kafkaConsumer = new KafkaConsumer<>(props);
        KafkaConsumer<String, Long> kafkaConsumer1 = new KafkaConsumer<>(props);

        //{"user_id":"2", "ranking_element_id":"324", "position":"0"}

        kafkaConsumer.subscribe(Collections.singletonList(topicREC));
        kafkaConsumer1.subscribe(Collections.singletonList(topicUC));



        while (true){
            ConsumerRecords<String, Long> records = kafkaConsumer.poll(Duration.ofMillis(100));

            //ConsumerRecord<String, Long> prev = null;
            for (ConsumerRecord<String, Long> current: records){

                /*ConsumerRecord<String, Long> temp = current;

                if(current.equals(temp)){

                }

                System.out.println("Key (ranking_element_id): " + current.key()
                        + ", Value (# of selections): " + current.value());
                System.out.println("Key (user_id): " + prev.key()
                        + ", Value (# of selections): " + prev.value());

                prev = temp;*/

                System.out.println("Key: " + current.key() + ", Value : " + current.value());
            }
        }
    }
}
