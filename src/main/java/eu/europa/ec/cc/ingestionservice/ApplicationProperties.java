package eu.europa.ec.cc.ingestionservice;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "application")
public class ApplicationProperties {

  private Map<String, Integer> providersThrottling;
  private String providerTopicPrefix;
  private String eventTopic;

  public void setProvidersThrottling(
      Map<String, Integer> providersThrottling) {
    this.providersThrottling = providersThrottling;
  }

  public Map<String, Integer> getProvidersThrottling() {
    return providersThrottling;
  }

  public String getProviderTopicPrefix() {
    return providerTopicPrefix;
  }

  public String getEventTopic() {
    return eventTopic;
  }

  public void setEventTopic(String eventTopic) {
    this.eventTopic = eventTopic;
  }

  public void setProviderTopicPrefix(String providerTopicPrefix) {
    this.providerTopicPrefix = providerTopicPrefix;
  }
}
