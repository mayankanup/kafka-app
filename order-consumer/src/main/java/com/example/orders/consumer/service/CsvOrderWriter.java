package com.example.orders.consumer.service;

import com.example.orders.common.Order;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Appends consumed orders to a CSV file.
 *
 * <p>Path is configured via {@code app.order.csv-path} (env {@code APP_ORDER_CSV_PATH}). Default is
 * {@code C:/temp/OrderDetails.csv} on Windows; use {@code /data/OrderDetails.csv} in Docker.
 */
@Component
public class CsvOrderWriter {

  private static final Logger log = LoggerFactory.getLogger(CsvOrderWriter.class);
  private static final String HEADER = "orderId,createdTime,orderTotal";

  private final Path csvPath;

  @Autowired
  public CsvOrderWriter(@Value("${app.order.csv-path:C:/temp/OrderDetails.csv}") String csvPath) {
    this.csvPath = Paths.get(csvPath);
  }

  // Test-only constructor
  CsvOrderWriter(Path csvPath) {
    this.csvPath = csvPath;
  }

  @PostConstruct
  void init() {
    try {
      if (csvPath.getParent() != null) {
        Files.createDirectories(csvPath.getParent());
      }
      if (Files.notExists(csvPath) || Files.size(csvPath) == 0) {
        Files.writeString(
            csvPath,
            HEADER + System.lineSeparator(),
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot init CSV file: " + csvPath, e);
    }
  }

  /** Thread-safe: Kafka listener threads share this bean. */
  public synchronized void append(Order order) {
    String line =
        String.join(
                ",",
                escape(order.getOrderId()),
                escape(order.getCreatedTime() == null ? "" : order.getCreatedTime().toString()),
                escape(order.getOrderTotal() == null ? "" : order.getOrderTotal().toPlainString()))
            + System.lineSeparator();
    try {
      if (csvPath.getParent() != null) {
        Files.createDirectories(csvPath.getParent());
      }
      Files.writeString(csvPath, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      log.info("Wrote order {} to {}", order.getOrderId(), csvPath);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot write CSV file: " + csvPath, e);
    }
  }

  private static String escape(String value) {
    if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }
}
