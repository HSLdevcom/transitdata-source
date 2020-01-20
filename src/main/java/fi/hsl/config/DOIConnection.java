package fi.hsl.config;

import org.apache.pulsar.shade.org.apache.commons.lang.NotImplementedException;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class DOIConnection {
    @PostConstruct
    public void init() {
        throw new NotImplementedException("Implement DOI Connection");
    }
}
