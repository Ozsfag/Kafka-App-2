package com.kafka_app.listeners;

import com.kafka_app.event.OrderEvent;
import com.kafka_app.event.OrderStatusEvent;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventListener {

  @Value("${app.kafka.order-status-event-topic}")
  private String topicName;

  private final KafkaTemplate<String, OrderStatusEvent> kafkaOrderStatusEventTemplate;

  @KafkaListener(
      topics = "${app.kafka.order-event-topic}",
      groupId = "app.kafka.order-event-group-id",
      containerFactory = "concurrentKafkaOrderStatusListenerContainerFactory")
  public void listenOrderEvent(@Payload OrderEvent orderEvent) {
    log.info("Receive orderEvent: {}", orderEvent);
    kafkaOrderStatusEventTemplate.send(topicName, new OrderStatusEvent("CREATED", Instant.now()));
  }
}
