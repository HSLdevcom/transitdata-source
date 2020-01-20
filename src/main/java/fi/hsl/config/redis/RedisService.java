package fi.hsl.config.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;

@Service
@Slf4j
public class RedisService {
    @Autowired
    private RedisTemplate<Object, Object> redisTemplate;

    @PostConstruct
    public void init() {
        log.info("Redis service starter");
    }

    public synchronized Map<Object, Object> hgetAll(String key) {
        return redisTemplate.opsForHash().entries(key);
    }

    public synchronized Object get(String key) {
        return redisTemplate.opsForValue().get(key);
    }
}
