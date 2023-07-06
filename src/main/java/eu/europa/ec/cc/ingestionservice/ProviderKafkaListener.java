package eu.europa.ec.cc.ingestionservice;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ConsumerSeekAware;

public class ProviderKafkaListener implements BatchMessageListener<byte[],byte[]>,
    ConsumerSeekAware {

  private final String topicName;
  private final ThrottlingController counter;
  private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
  private final String eventTopic;

  public ProviderKafkaListener(
      String topicName,
      ThrottlingController counter,
      KafkaTemplate<byte[],byte[]> kafkaTemplate,
      String eventTopic) {
    this.topicName = topicName;
    this.counter = counter;
    this.kafkaTemplate = kafkaTemplate;
    this.eventTopic = eventTopic;
  }

  @Override
  public void onMessage(List<ConsumerRecord<byte[], byte[]>> data) {
    try {
      for (ConsumerRecord<byte[], byte[]> record : data) {
        long sleepTime = counter.findThrottlingDelay(topicName, record.partition());
        if (sleepTime != 0){
          Thread.sleep(sleepTime);
        }
        kafkaTemplate.send(eventTopic, record.key(), record.value());
        counter.onMessageSent(topicName, record.partition());
      }
    } catch (InterruptedException e){
      throw new RuntimeException(e);
    }
  }

}
