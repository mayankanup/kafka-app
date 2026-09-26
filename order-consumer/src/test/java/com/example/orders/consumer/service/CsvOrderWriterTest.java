package com.example.orders.consumer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.orders.common.Order;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvOrderWriterTest {

  @TempDir Path tempDir;

  @Test
  void writesHeaderThenAppendsRows() throws Exception {
    Path csv = tempDir.resolve("OrderDetails.csv");
    CsvOrderWriter writer = new CsvOrderWriter(csv);
    writer.init();

    writer.append(new Order("id-1", Instant.parse("2026-01-01T10:00:00Z"), new BigDecimal("199.99")));
    writer.append(new Order("id-2", Instant.parse("2026-01-02T11:00:00Z"), new BigDecimal("10.50")));

    List<String> lines = Files.readAllLines(csv);
    assertEquals(3, lines.size());
    assertEquals("orderId,createdTime,orderTotal", lines.get(0));
    assertTrue(lines.get(1).startsWith("id-1,2026-01-01T10:00:00Z,199.99"));
    assertTrue(lines.get(2).startsWith("id-2,2026-01-02T11:00:00Z,10.50"));
  }
}
