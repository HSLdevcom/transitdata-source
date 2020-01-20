package fi.hsl.pubtrans;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.config.databases.redis.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
class PubTransCache {
    @Autowired
    private RedisService redisService;

    String getStopId(long jppId) {
        String stopIdKey = TransitdataProperties.REDIS_PREFIX_JPP + jppId;
        return (String) redisService.get(stopIdKey);
    }

    Map<String, String> getTripInfoFields(long dvjId) {
        String tripInfoKey = TransitdataProperties.REDIS_PREFIX_DVJ + dvjId;
        return redisService.hgetAll(tripInfoKey);
    }
}
