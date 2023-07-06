package eu.europa.ec.cc.ingestionservice;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
public class ProviderKafkaListenerConfigurer implements
    ApplicationListener<ContextRefreshedEvent> {

  private final ThrottlingController counter;
  private final ConcurrentKafkaListenerContainerFactory<?,?> factory;
  private final KafkaTemplate<byte[],byte[]> kafkaTemplate;
  private final ApplicationProperties applicationProperties;

  public ProviderKafkaListenerConfigurer(ThrottlingController counter,
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
          String topicName = applicationProperties.getProviderTopicPrefix() + provider;
          ConcurrentMessageListenerContainer<?, ?> container = factory.createContainer(
              topicName);

          container.getContainerProperties().setMessageListener(new ProviderKafkaListener(
              topicName,
              counter,
              kafkaTemplate,
              applicationProperties.getEventTopic()
          ));
          container.setBeanName("listener-"+topicName);
          container.start();
        }
    );
  }
}
