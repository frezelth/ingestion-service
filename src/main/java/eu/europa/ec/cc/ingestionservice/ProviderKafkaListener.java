package eu.europa.ec.cc.ingestionservice;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchMessageListener;

public class ProviderKafkaListener implements BatchMessageListener<byte[],byte[]> {

  private final String provider;
  private final MessageCounter counter;
  private final KafkaTemplate<byte[], byte[]> kafkaTemplate;

  public ProviderKafkaListener(
      String provider,
      MessageCounter counter,
      KafkaTemplate<byte[],byte[]> kafkaTemplate) {
    this.provider = provider;
    this.counter = counter;
    this.kafkaTemplate = kafkaTemplate;
  }

  @Override
  public void onMessage(List<ConsumerRecord<byte[], byte[]>> data) {
    try {
      for (ConsumerRecord<byte[], byte[]> record : data) {
        while (!counter.canSendMessage(provider)) {
          Thread.sleep(30);
        }
        kafkaTemplate.send("cc_dev_event", record.key(), record.value());
        counter.onMessageSent(provider);
      }
    } catch (InterruptedException e){
      throw new RuntimeException(e);
    }
  }

}
