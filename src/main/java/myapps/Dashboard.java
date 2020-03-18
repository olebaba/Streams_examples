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

import java.util.Properties;

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

        String topic0 = "user_count", topic1 = "ranking_element_count", topic2 = "campaign_count";

        KStream<String, JsonNode> userStream0 = builder0
                .stream(topic0, Consumed.with(Serdes.String(), jsonNodeSerde));
        KStream<String, JsonNode> userStream1 = builder1
                .stream(topic1, Consumed.with(Serdes.String(), jsonNodeSerde));
        KStream<String, JsonNode> userStream2 = builder2
                .stream(topic2, Consumed.with(Serdes.String(), jsonNodeSerde));

        //userStream0.
    }
}
