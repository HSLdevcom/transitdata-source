package fi.hsl.config.redis;

import org.apache.pulsar.shade.org.apache.commons.lang.NotImplementedException;
import org.springframework.stereotype.Service;

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
