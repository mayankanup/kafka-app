package com.example.orders.consumer.consumer;

import com.example.orders.common.Order;
import com.example.orders.consumer.service.CsvOrderWriter;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

  private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

  private final CsvOrderWriter csvWriter;
  private final long minDelayMs;
  private final long maxDelayMs;

  public OrderConsumer(
      CsvOrderWriter csvWriter,
      @Value("${app.order.processing.min-delay-ms:20000}") long minDelayMs,
      @Value("${app.order.processing.max-delay-ms:60000}") long maxDelayMs) {
    this.csvWriter = csvWriter;
    this.minDelayMs = minDelayMs;
    this.maxDelayMs = maxDelayMs;
  }

  @KafkaListener(
      topics = "${app.order.topic:orders}",
      groupId = "${spring.kafka.consumer.group-id:order-processor}")
  public void onOrder(ConsumerRecord<String, Order> record, Consumer<String, Order> consumer) {
    Order order = record.value();
    long backlog = estimateBacklog(consumer);
    log.info(
        "Processing order id={} partition={} offset={} | ~{} orders waiting in queue",
        order.getOrderId(),
        record.partition(),
        record.offset(),
        backlog < 0 ? "unknown" : backlog);

    // Simulate work before persisting.
    long workMs = ThreadLocalRandom.current().nextLong(minDelayMs, maxDelayMs + 1);
    try {
      Thread.sleep(workMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    csvWriter.append(order);
    log.info(
        "Processed offset {} (order {}) in {} ms -> CSV",
        record.offset(),
        order.getOrderId(),
        workMs);
  }

  /** Approximate unread backlog across assigned partitions (end offset - current position). */
  private long estimateBacklog(Consumer<String, Order> consumer) {
    try {
      long total = 0;
      for (TopicPartition tp : consumer.assignment()) {
        long end = consumer.endOffsets(List.of(tp)).getOrDefault(tp, -1L);
        long pos = consumer.position(tp);
        if (end >= 0) {
          total += Math.max(0, end - pos);
        }
      }
      return total;
    } catch (Exception e) {
      log.warn("Could not estimate queue backlog", e);
      return -1;
    }
  }
}
