package com.example.orders.producer.producer;

import com.example.orders.common.Order;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * CLI loop: publishes up to {@code app.order.max-orders} orders with a random delay between them,
 * then the app exits.
 */
@Component
public class OrderRunner implements CommandLineRunner {

  private static final Logger log = LoggerFactory.getLogger(OrderRunner.class);

  private final OrderProducer producer;
  private final int maxOrders;
  private final long minDelayMs;
  private final long maxDelayMs;

  public OrderRunner(
      OrderProducer producer,
      @Value("${app.order.max-orders:100}") int maxOrders,
      @Value("${app.order.min-delay-ms:1000}") long minDelayMs,
      @Value("${app.order.max-delay-ms:30000}") long maxDelayMs) {
    this.producer = producer;
    this.maxOrders = maxOrders;
    this.minDelayMs = minDelayMs;
    this.maxDelayMs = maxDelayMs;
  }

  @Override
  public void run(String... args) {
    log.info(
        "Producer starting: up to {} orders, random delay {}-{} ms between orders",
        maxOrders,
        minDelayMs,
        maxDelayMs);
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    int sent = 0;
    for (int i = 1; i <= maxOrders; i++) {
      Order order = newOrder();
      try {
        SendResult<String, Order> r = producer.sendAndWait(order, Duration.ofSeconds(30));
        sent++;
        log.info(
            "Produced order {}/{} id={} partition={} offset={}",
            i,
            maxOrders,
            order.getOrderId(),
            r.getRecordMetadata().partition(),
            r.getRecordMetadata().offset());
      } catch (Exception e) {
        log.error("Failed to publish order {}/{} id={}", i, maxOrders, order.getOrderId(), e);
      }
      if (i < maxOrders) {
        long sleepMs = rnd.nextLong(minDelayMs, maxDelayMs + 1);
        log.info("Sleeping {} ms before next order...", sleepMs);
        try {
          Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    log.info("Producer finished: {}/{} orders published. Exiting.", sent, maxOrders);
  }

  static Order newOrder() {
    BigDecimal total =
        BigDecimal.valueOf(ThreadLocalRandom.current().nextDouble(10.0, 1000.0))
            .setScale(2, RoundingMode.HALF_UP);
    return new Order(UUID.randomUUID().toString(), Instant.now(), total);
  }
}
