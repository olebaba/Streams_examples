package Classes;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class RankingDeserializer implements Deserializer<RankingElements> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public RankingElements deserialize(String s, byte[] bytes) {
        if(bytes == null) return null;

        try {
            return new ObjectMapper().readValue(bytes, RankingElements.class);
        } catch (IOException jpe){
            jpe.printStackTrace();
        }

        return null;
    }

    @Override
    public RankingElements deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}
