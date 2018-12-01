package com.nadav.kafka.server.consumer;

import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

public class Main {

    public static void main(String[] args) {

    String bootstrapServer = "127.0.0.1:9092";
    String groupId = "kafka-server-consumer-first-application";
    String topic = "first_topic";
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

    KafkaServerConsumer kafkaServerConsumer = KafkaServerConsumer.getInstance(properties, Collections.singleton(topic));
    kafkaServerConsumer.registerToKafkaEvent(topic,record ->{
            System.out.println("key: " + record.key());
            System.out.println("value: " + record.value());
    });
    kafkaServerConsumer.listen();
    }
}

