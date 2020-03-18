package myapps;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
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


            KTable<String, Long> userCount = rankingSource
                    .filter((s1, jsonNode1) -> jsonNode1.has("user_id"))
                    .groupBy((s1, jsonNode1) -> jsonNode1.get("user_id").textValue())
                    .count(Materialized.as("user-store"));

            KTable<String, Long> rankingElementCount = rankingSource
                    .filter((s, jsonNode) -> jsonNode.has("ranking_element_id"))
                    .groupBy((s, jsonNode) -> jsonNode.get("ranking_element_id").textValue())
                    .count(Materialized.as("element-store"));

            //KTable<String, String> joined = rankingElementCount.join(userCount,
            //        (aLong, aLong2) -> "Ranking element = " + aLong + ", User id = " + aLong);

            //joined.toStream().to("ranking_element_count", Produced.with(Serdes.String(), Serdes.String()));

            rankingSource.foreach((s, jsonNode) -> {
                System.out.println(jsonNode.toPrettyString());
            });

            userCount.toStream().to("user_count", Produced.with(Serdes.String(), Serdes.Long()));
            rankingElementCount.toStream().to("ranking_element_count", Produced.with(Serdes.String(), Serdes.Long()));


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