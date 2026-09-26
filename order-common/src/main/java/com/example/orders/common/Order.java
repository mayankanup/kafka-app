package com.example.orders.common;

import java.math.BigDecimal;
import java.time.Instant;

/** Order event produced to Kafka and persisted to CSV by the consumer. Shared by both services. */
public class Order {

  private String orderId;
  private Instant createdTime;
  private BigDecimal orderTotal;

  public Order() {}

  public Order(String orderId, Instant createdTime, BigDecimal orderTotal) {
    this.orderId = orderId;
    this.createdTime = createdTime;
    this.orderTotal = orderTotal;
  }

  public String getOrderId() {
    return orderId;
  }

  public void setOrderId(String orderId) {
    this.orderId = orderId;
  }

  public Instant getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(Instant createdTime) {
    this.createdTime = createdTime;
  }

  public BigDecimal getOrderTotal() {
    return orderTotal;
  }

  public void setOrderTotal(BigDecimal orderTotal) {
    this.orderTotal = orderTotal;
  }
}
