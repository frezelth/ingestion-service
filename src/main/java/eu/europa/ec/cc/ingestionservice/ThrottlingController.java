package eu.europa.ec.cc.ingestionservice;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public class ThrottlingController {

  private final static Logger LOGGER = LoggerFactory.getLogger(ThrottlingController.class);
  private final Map<String, Integer> thresholdPerTopicPartition;
  private final Map<TopicPartition, AtomicInteger> countPerTopicPartition;
  private static Instant lastResetInstant = Instant.now();

  public ThrottlingController(Map<String, Integer> maxMessagesPerPartitionPerSecond) {
    // initialize topic per partition count
    this.countPerTopicPartition = new ConcurrentHashMap<>();
    this.thresholdPerTopicPartition = maxMessagesPerPartitionPerSecond;
  }

  public void onMessageSent(String topic, int partition){
    if (LOGGER.isTraceEnabled()){
      LOGGER.trace("Message sent for topic {} on partition {}", topic, partition);
    }
    countPerTopicPartition.computeIfAbsent(new TopicPartition(topic, partition), (k) -> new AtomicInteger()).addAndGet(1);
  }

  public long findThrottlingDelay(String topic, int partition){
    if (countPerTopicPartition.getOrDefault(new TopicPartition(topic, partition), new AtomicInteger()).get() >= thresholdPerTopicPartition.get(topic)){
      long millisThrottling = Duration.between(Instant.now(), lastResetInstant.plusSeconds(1)).toMillis();
      if (LOGGER.isTraceEnabled()){
        LOGGER.trace("Throttling topic {} on partition {} for {}", topic, partition, millisThrottling);
      }
      return millisThrottling;
    }
    if (LOGGER.isTraceEnabled()){
      LOGGER.trace("No throttling for topic {} on partition {}", topic, partition);
    }
    return 0;
  }

  // reset the shared counter
  @Scheduled(fixedRate = 1, timeUnit = TimeUnit.SECONDS)
  public void clearCounter(){
    lastResetInstant = Instant.now();
    countPerTopicPartition.values().forEach(
        atomicInteger -> atomicInteger.set(0)
    );
  }
}
