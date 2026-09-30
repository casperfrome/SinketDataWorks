package com.fake.dataworks.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class JsonCodec {
    private final Gson gson = new GsonBuilder().serializeNulls().create();
    public String write(Object value) { return gson.toJson(value); }
    public <T> T read(String value,Class<T> type) { return gson.fromJson(value,type); }
    public Map<String,Object> map(String json) { return gson.fromJson(json,new TypeToken<Map<String,Object>>(){}.getType()); }
    public List<String> strings(String json) { return gson.fromJson(json,new TypeToken<List<String>>(){}.getType()); }
}
