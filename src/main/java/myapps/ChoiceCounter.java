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
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

            final StreamsBuilder builder = new StreamsBuilder();

            final Serializer<JsonNode> nodeSerializer = new JsonSerializer();
            final Deserializer<JsonNode> nodeDeserializer = new JsonDeserializer();
            final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(nodeSerializer, nodeDeserializer);

            String topic = "ranking_element_choices";//"user-input";//

            KStream<String, JsonNode> rankingSource = builder
                    .stream(topic, Consumed.with(Serdes.String(), jsonNodeSerde));

            rankingSource.foreach((s, jsonNode) -> System.out.println(jsonNode.toPrettyString()));

            //rankingSource.to("ranking_element_count"); //Pipe


            KTable<Long, Long> userCount = rankingSource
                    .filter((s1, jsonNode) -> jsonNode.has("user_id"))
                    .groupBy((s1, jsonNode) -> jsonNode.get("user_id").longValue())
                    .count(Materialized.as("user-store"));

            KTable<Long, Long> rankingElementCount = rankingSource
                    .filter((s, jsonNode) -> jsonNode.has("ranking_element_id"))
                    .groupBy((s, jsonNode) -> jsonNode.get("ranking_element_id").longValue())
                    .count(Materialized.as("ranking-element-store"));

            KTable<Long, Long> campaignCount = rankingSource
                    .filter((s, jsonNode) -> jsonNode.has("campaign_id"))
                    .groupBy((s, jsonNode) -> jsonNode.get("campaign_id").longValue())
                    .count(Materialized.as("campaign-store"));


            userCount.toStream().to("user_count", Produced.with(Serdes.Long(), Serdes.Long()));
            rankingElementCount.toStream().to("ranking_element_count", Produced.with(Serdes.Long(), Serdes.Long()));
            campaignCount.toStream().to("campaign_count", Produced.with(Serdes.Long(), Serdes.Long()));


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