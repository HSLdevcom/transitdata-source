package fi.hsl.config.databases;

import org.apache.pulsar.shade.org.apache.commons.lang.NotImplementedException;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class ROIConnection {
    @PostConstruct
    public void init() {
        //TODO
        throw new NotImplementedException("Implement ROI connection");
    }
}
