package eu.europa.ec.cc.ingestionservice;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public class MessageCounter {

  private final static Logger LOGGER = LoggerFactory.getLogger(MessageCounter.class);
  private final Map<String, Integer> thresholdPerProvider;
  private final Map<String, AtomicInteger> countPerProvider;

  public MessageCounter(Map<String, Integer> thresholdPerProvider) {
    this.countPerProvider = thresholdPerProvider.keySet().stream()
        .collect(Collectors.toMap(
            s -> s, s -> new AtomicInteger()
        ));
    this.thresholdPerProvider = thresholdPerProvider.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey, Entry::getValue
        ));
  }

  public void onMessageSent(String provider){
    if (LOGGER.isDebugEnabled()){
      LOGGER.debug("Sending message for {}", provider);
    }
    countPerProvider.get(provider).addAndGet(1);
  }

  public boolean canSendMessage(String provider){
    boolean decision = countPerProvider.get(provider).get() < thresholdPerProvider.get(provider);
    if (LOGGER.isDebugEnabled() && decision){
      LOGGER.debug("Message count for provider {} : {}, decision: {}", provider, countPerProvider.get(provider), decision);
    }
    return decision;
  }

  // reset the shared counter
  @Scheduled(fixedRate = 1, timeUnit = TimeUnit.SECONDS)
  public void clearCounter(){
    countPerProvider.values().forEach(
        atomicInteger -> atomicInteger.set(0)
    );
  }
}
