package fi.hsl.config.databases.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
public class RedisConnection {
    @Value("${redis.hostname}")
    private String redisHost;

    @Value("${redis.port}")
    private Integer redisPort;

    @Bean
    public RedisConnectionFactory connectionFactory(RedisStandaloneConfiguration redisStandaloneConfiguration,
                                                    LettucePoolingClientConfiguration lettucePoolConfig) {
        return new LettuceConnectionFactory(redisStandaloneConfiguration, lettucePoolConfig);
    }

    @Bean
    public RedisStandaloneConfiguration redisStandaloneConfiguration() {
        return new RedisStandaloneConfiguration(redisHost, redisPort);
    }

    @Bean
    @Primary
    public RedisTemplate<Object, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        return new RedisTemplate<>() {{
            setConnectionFactory(redisConnectionFactory);
        }};
    }

    @Bean
    LettucePoolingClientConfiguration lettucePoolConfig() {
        return LettucePoolingClientConfiguration.builder()
                .poolConfig(new GenericObjectPoolConfig())
                .clientOptions(clientOptions())
                .clientResources(clientResources())
                .build();
    }

    @Bean(destroyMethod = "shutdown")
    ClientResources clientResources() {
        return DefaultClientResources.create();
    }

    @Bean
    ClientOptions clientOptions() {
        return ClientOptions.builder()
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .autoReconnect(true)
                .build();
    }

}
