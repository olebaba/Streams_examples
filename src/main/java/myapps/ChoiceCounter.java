package myapps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ChoiceCounter {

    public static void main(String[] args) throws Exception {
        try {


            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-choice-counter");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

            final StreamsBuilder builder = new StreamsBuilder();

            final Serializer<JsonNode> nodeSerializer = new JsonSerializer();
            final Deserializer<JsonNode> nodeDeserializer = new JsonDeserializer();
            final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(nodeSerializer, nodeDeserializer);

            String topic = "user-input3";//"ranking_element_choices";


            KStream<String, JsonNode> rankingSource = builder
                    .stream(topic, Consumed.with(Serdes.String(), jsonNodeSerde));

            //rankingSource.to("ranking_element_count"); //Pipe

            //ObjectMapper om = new ObjectMapper();

            KStream<String, Long> rankingCount = rankingSource
                    .filter((s, jsonNode) -> jsonNode != null)
                    .groupBy((s, jsonNode) -> jsonNode.get("user_id").textValue())
                    .count(Materialized.as("counting-store"))
                    .toStream();

            rankingSource.foreach((s, jsonNode) -> {
                System.out.println(jsonNode.toPrettyString());
            });

            //
            // Consumer<Long, String> consumer = new KafkaConsumer<>(props);
            //consumer.subscribe(Collections.singleton("ranking_element_count"));

            rankingCount.to("ranking_element_count", Produced.with(Serdes.String(), Serdes.Long()));


            final Topology topology = builder.build();
            final KafkaStreams streams = new KafkaStreams(topology, props);
            final CountDownLatch latch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
                    latch.countDown();
                }
            });

            try {
                streams.start();
                latch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
            System.exit(0);
        }catch (Exception e){
            System.out.println(Arrays.toString(e.getStackTrace()));
        }
    }
}