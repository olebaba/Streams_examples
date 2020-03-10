package Classes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class JsonNodeDeserializer implements Deserializer<JsonNode> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public JsonNode deserialize(String topic, byte[] data) {

        try {
            return mapper.readValue(data, JsonNode.class);
        } catch (IOException e) {
            return null;
        }
    }
}