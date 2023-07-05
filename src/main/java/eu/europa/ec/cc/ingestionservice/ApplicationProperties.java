package eu.europa.ec.cc.ingestionservice;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "application")
public class ApplicationProperties {

  private Map<String, Integer> providersThrottling;

  public void setProvidersThrottling(
      Map<String, Integer> providersThrottling) {
    this.providersThrottling = providersThrottling;
  }

  public Map<String, Integer> getProvidersThrottling() {
    return providersThrottling;
  }
}
