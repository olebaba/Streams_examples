package myapps;

import Classes.RankingDeserializer;
import Classes.RankingElements;
import Classes.RankingSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ChoiceCounter {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-choice-counter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        final Serializer<RankingElements> rankingElementsSerializer = new RankingSerializer();
        final Deserializer<RankingElements> rankingElementsDeserializer = new RankingDeserializer();
        final Serde<RankingElements> rankingElementsSerde =
                Serdes.serdeFrom(rankingElementsSerializer, rankingElementsDeserializer);

        KStream<String, RankingElements> rankingSource = builder
                .stream("ranking_element_choices", Consumed.with(Serdes.String(), rankingElementsSerde));

        //rankingSource.map((s, rankingElements) -> new KeyValue<>(rankingElements.rankingElementId, rankingElements))
        //        .groupBy((integer, rankingElements) -> )


        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        }catch (Throwable e){
            System.exit(1);
        }
        System.exit(0);
    }
}