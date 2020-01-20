package fi.hsl.config.redis;

import org.apache.pulsar.shade.org.apache.commons.lang.NotImplementedException;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class RedisConnection {
    @PostConstruct
    public void init() {
        throw new NotImplementedException();
    }
}
