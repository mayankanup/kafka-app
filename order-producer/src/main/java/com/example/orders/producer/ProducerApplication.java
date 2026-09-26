package com.example.orders.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Producer process (CLI): publishes up to 100 orders to Kafka, then exits. */
@SpringBootApplication
public class ProducerApplication {
  public static void main(String[] args) {
    SpringApplication.run(ProducerApplication.class, args);
  }
}
