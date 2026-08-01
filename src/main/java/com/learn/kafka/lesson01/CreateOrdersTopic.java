package com.learn.kafka.lesson01;

import com.learn.kafka.common.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

/**
 * Lesson 1 — create the orders topic with 3 partitions and replication factor 1.
 *
 * Why 3 partitions? So we can see keys land in different partitions in the next step.
 */
public class CreateOrdersTopic {

  public static final String TOPIC = "orders";

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    try (AdminClient admin = AdminClient.create(KafkaConfig.base())) {
      NewTopic orders = new NewTopic(TOPIC, 3, (short) 1);
      admin.createTopics(Collections.singleton(orders)).all().get();
      System.out.println("Created topic '" + TOPIC + "' with 3 partitions (RF=1)");
    } catch (ExecutionException e) {
      if (e.getCause() != null && e.getCause().getClass().getSimpleName().contains("TopicExists")) {
        System.out.println("Topic '" + TOPIC + "' already exists — OK");
      } else {
        throw e;
      }
    }
  }
}
