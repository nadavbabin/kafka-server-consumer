package com.nadav.kafka.server.consumer;

import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.function.Consumer;

public class ConsumerThread implements Runnable {

    private CountDownLatch latch;
    private KafkaConsumer<String,String> consumer;
    private MultiKeyMap<String,Consumer<ConsumerRecord<String,String>>> register;
    private final int numOfThreads = 200;
    private ForkJoinPool forkJoinPool = new ForkJoinPool(numOfThreads);
    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

    public ConsumerThread(CountDownLatch latch,Collection topics
    ,String bootstrapServer,String groupId,String offset){
        register = new MultiKeyMap<>();
        this.latch = latch;
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,String.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,String.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offset);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);
    }

    public ConsumerThread(CountDownLatch latch, Properties properties, Collection topics){
        register = new MultiKeyMap<>();
        this.latch = latch;
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);
    }

    public void registerToEvent(String topic , String key , Consumer<ConsumerRecord<String,String>> func){
        register.put(topic,key,func);
    }

    public void run() {
        ConsumerRecords<String,String> consumerRecords;
        try{
            while (true){
                consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<String,String> record : consumerRecords){
                    if (register != null){
                        forkJoinPool.invoke(new RecursiveAction() {
                            @Override
                            protected void compute() {
                                register.get(record.topic(),record.key()).accept(record);
                            }
                        });
                    }
                }
            }
        }
        catch (WakeupException e){
            logger.info("wakeup exception",e);
        }
        finally {
            consumer.close();
            latch.countDown();
            logger.info("close application");
        }
    }

    public void shutdown(){
        logger.info("called to shutdown medthod");
        consumer.wakeup();
    }
}
