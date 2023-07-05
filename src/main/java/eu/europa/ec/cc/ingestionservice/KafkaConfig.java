package eu.europa.ec.cc.ingestionservice;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

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
            .name("cc_dev_provider-" + provider + "-event")
            .partitions(1)
            .replicas(1)
            .build())
        .toList().toArray(NewTopic[]::new));
  }

  @Bean
  public NewTopic eventTopic(){
    return TopicBuilder
        .name("cc_dev_event")
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
  public MessageCounter counter(ApplicationProperties applicationProperties){
    return new MessageCounter(applicationProperties.getProvidersThrottling());
  }

  @Component
  static class CreateAndStartProviderListeners implements ApplicationListener<ContextRefreshedEvent> {

    private final MessageCounter counter;
    private final ConcurrentKafkaListenerContainerFactory<?,?> factory;
    private final KafkaTemplate<byte[],byte[]> kafkaTemplate;
    private final ApplicationProperties applicationProperties;

    public CreateAndStartProviderListeners(MessageCounter counter,
        ConcurrentKafkaListenerContainerFactory<?,?> factory,
        KafkaTemplate<byte[], byte[]> kafkaTemplate,
        ApplicationProperties applicationProperties) {
      this.counter = counter;
      this.factory = factory;
      this.kafkaTemplate = kafkaTemplate;
      this.applicationProperties = applicationProperties;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
      applicationProperties.getProvidersThrottling().keySet().forEach(
          provider -> {
            ConcurrentMessageListenerContainer<?, ?> container = factory.createContainer(
                "cc_dev_provider-" + provider + "-event");

            container.getContainerProperties().setMessageListener(new ProviderKafkaListener(
                provider,
                counter,
                kafkaTemplate
            ));
            container.setBeanName("listener-"+provider);
            container.start();
          }
      );
    }
  }

}
