package testjoin.com.test.kafka;


import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import com.google.gson.Gson;


import java.nio.charset.Charset;


public class JsonSerializer<T> implements Serializer<T> {

    private Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, T t) {
        return gson.toJson(t).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public void close() {

    }
    
    public JsonSerializer() {}
    

}
