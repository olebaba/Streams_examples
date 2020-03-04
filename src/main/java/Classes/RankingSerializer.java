package Classes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class RankingSerializer implements Serializer<RankingElements> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, RankingElements rankingElements) {
        if(rankingElements == null) return null;

        try {
            return new ObjectMapper().writeValueAsBytes(rankingElements);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public byte[] serialize(String topic, Headers headers, RankingElements data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
