package com.kafka_app.listeners;

import com.kafka_app.event.OrderEvent;
import com.kafka_app.event.OrderStatusEvent;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventListener {
  private final KafkaTemplate<String, OrderStatusEvent> kafkaOrderStatusEventTemplate;

  @KafkaListener(
      topics = "${app.kafka.order-event-topic}",
      groupId = "app.kafka.order-event-group-id")
  public void listenOrderEvent(OrderEvent orderEvent) {
    log.info("Receive orderEvent: {}", orderEvent);
    kafkaOrderStatusEventTemplate.send(
        "${app.kafka.order-status-event-topic}", new OrderStatusEvent("CREATED", Instant.now()));
  }
}
