package myapps;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.*;

public class Dashboard {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-choice-counter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        final StreamsBuilder builder0 = new StreamsBuilder();
        final StreamsBuilder builder1 = new StreamsBuilder();
        final StreamsBuilder builder2 = new StreamsBuilder();

        final Serializer<JsonNode> nodeSerializer = new JsonSerializer();
        final Deserializer<JsonNode> nodeDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(nodeSerializer, nodeDeserializer);

        List<Map<String, Long>> streams = new LinkedList<>();
        Map<String, Long> userData = new HashMap<>();
        Map<String, Long> rankingElementCount = new HashMap<>();
        Map<String, Long> campaignCount = new HashMap<>();
        streams.add(userData);
        streams.add(rankingElementCount);
        streams.add(campaignCount);

        String topic0 = "user_count", topic1 = "ranking_element_count", topic2 = "campaign_count";

        KStream<String, Long> userStream0 = builder0.stream(topic0, Consumed.with(Serdes.String(), Serdes.Long()));
        KStream<String, Long> userStream1 = builder1.stream(topic1, Consumed.with(Serdes.String(), Serdes.Long()));
        KStream<String, Long> userStream2 = builder2.stream(topic2, Consumed.with(Serdes.String(), Serdes.Long()));

        while (true) {
            userStream0.mapValues(userData::put);
            userStream1.mapValues(rankingElementCount::put);
            userStream2.mapValues(campaignCount::put);
        }
    }
}
