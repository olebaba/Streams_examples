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
        ObjectMapper objectMapper = new ObjectMapper();
        RankingElements rankingElements = null;
        try{
            rankingElements = objectMapper.readValue(bytes, RankingElements.class);
        }catch (Exception e){
            e.printStackTrace();
        }
        return rankingElements;
    }

    @Override
    public void close() {

    }
}
