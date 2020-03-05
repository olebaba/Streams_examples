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
        byte[] retval = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try{
            retval = objectMapper.writeValueAsString(rankingElements).getBytes();
        }catch (Exception e){
            e.printStackTrace();
        }
        return retval;
    }

    @Override
    public void close() {

    }
}
