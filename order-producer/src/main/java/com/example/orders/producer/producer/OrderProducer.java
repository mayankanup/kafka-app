package com.example.orders.producer.producer;

import com.example.orders.common.Order;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

  private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

  private final KafkaTemplate<String, Order> kafkaTemplate;
  private final String topic;

  public OrderProducer(
      KafkaTemplate<String, Order> kafkaTemplate,
      @Value("${app.order.topic:orders}") String topic) {
    this.kafkaTemplate = kafkaTemplate;
    this.topic = topic;
  }

  public void send(Order order) {    CompletableFuture<SendResult<String, Order>> future =
        kafkaTemplate.send(topic, order.getOrderId(), order);
    future.whenComplete(
        (result, ex) -> {
          if (ex != null) {
            log.error("Failed to send order {}", order.getOrderId(), ex);
          } else {
            log.info(
                "Sent order {} to {}-{}@{}",
                order.getOrderId(),
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
          }
        });
  }

  /** Sends and blocks until the broker acknowledges (used by the CLI loop). */
  public SendResult<String, Order> sendAndWait(Order order, Duration timeout) throws Exception {
    try {
      return kafkaTemplate
          .send(topic, order.getOrderId(), order)
          .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new TimeoutException("Timed out publishing order " + order.getOrderId());
    }
  }
}
