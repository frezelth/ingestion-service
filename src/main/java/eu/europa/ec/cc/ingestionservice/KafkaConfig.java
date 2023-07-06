package eu.europa.ec.cc.ingestionservice;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@EnableKafka
public class KafkaConfig {

  private final ApplicationProperties applicationProperties;

  public KafkaConfig(ApplicationProperties applicationProperties) {
    this.applicationProperties = applicationProperties;
  }

  @Bean
  public NewTopics providerTopics() {
    return new NewTopics(applicationProperties.getProvidersThrottling().keySet().stream()
        .map(provider -> TopicBuilder
            .name(applicationProperties.getProviderTopicPrefix() + provider)
            .partitions(3)
            .replicas(1)
            .build())
        .toList().toArray(NewTopic[]::new));
  }

  @Bean
  public NewTopic eventTopic(){
    return TopicBuilder
        .name(applicationProperties.getEventTopic())
        .partitions(1)
        .replicas(1)
        .build();
  }

  @Bean
  public KafkaTemplate<byte[],byte[]> kafkaTemplate(
      ProducerFactory<byte[],byte[]> producerFactory
  ){
    return new KafkaTemplate<>(producerFactory);
  }

  @Bean
  public ThrottlingController counter(ApplicationProperties applicationProperties){
    // try to load partition information for all provider topics
    Map<String, Integer> throttlingPerProviderTopic = applicationProperties.getProvidersThrottling()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(e -> applicationProperties.getProviderTopicPrefix() + e.getKey(), Entry::getValue));

    return new ThrottlingController(throttlingPerProviderTopic);
  }

}
