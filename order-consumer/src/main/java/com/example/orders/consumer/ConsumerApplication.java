package com.example.orders.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Consumer process: reads orders from Kafka and appends them to a CSV file. No HTTP port. */
@SpringBootApplication
public class ConsumerApplication {
  public static void main(String[] args) {
    SpringApplication.run(ConsumerApplication.class, args);
  }
}
