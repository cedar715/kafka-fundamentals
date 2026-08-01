package com.learn.kafka.lesson01;

import com.learn.kafka.common.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Lesson 1 — write messages to the orders topic and print partition + offset.
 *
 * Run CreateOrdersTopic first, then this class.
 * Watch which partition each key lands in: same key → same partition.
 */
public class SimpleProducer {

  public static void main(String[] args) throws Exception {
    Properties props = KafkaConfig.base();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // Wait for leader ack — good default while learning durability later
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      String[] customerIds = {"cust-1", "cust-2", "cust-1", "cust-3", "cust-2"};

      for (int i = 0; i < customerIds.length; i++) {
        String key = customerIds[i];
        String value = "order-" + (i + 1) + " for " + key;

        ProducerRecord<String, String> record =
            new ProducerRecord<>(CreateOrdersTopic.TOPIC, key, value);

        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata meta = future.get(); // block so we can print results clearly

        System.out.printf(
            "sent key=%-7s value=%-22s → partition=%d offset=%d%n",
            key, value, meta.partition(), meta.offset());
      }
    }
  }
}
