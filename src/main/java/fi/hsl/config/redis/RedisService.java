package fi.hsl.config.redis;

import org.springframework.stereotype.Service;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Map;

@Service
public class RedisService {
    public synchronized Map<String, String> hgetAll(String key) {
        throw new NotImplementedException();
    }

    public synchronized String get(String key) {
        throw new NotImplementedException();
    }
}
