package com.learn.kafka.common;

import java.util.Properties;

/**
 * Shared bootstrap settings for local Docker Kafka.
 */
public final class KafkaConfig {

  public static final String BOOTSTRAP_SERVERS = "localhost:9092";

  private KafkaConfig() {}

  public static Properties base() {
    Properties props = new Properties();
    props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
    return props;
  }
}
