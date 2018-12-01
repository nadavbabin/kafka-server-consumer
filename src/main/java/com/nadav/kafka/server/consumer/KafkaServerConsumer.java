package com.nadav.kafka.server.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class KafkaServerConsumer {
    private CountDownLatch latch;
    private ConsumerThread consumerThread;
    private Thread runThread;
    private Logger logger = LoggerFactory.getLogger(KafkaServerConsumer.class);
    private static KafkaServerConsumer instance = null;

    public static KafkaServerConsumer getInstance(Properties properties, Collection topics){
        if (instance == null){
            synchronized (KafkaServerConsumer.class){
                if (instance == null){
                    instance = new KafkaServerConsumer(properties,topics);
                }
            }
        }
        return instance;
    }

    public static KafkaServerConsumer getInstance(Collection topics,String bootstrapServer,String groupId,String offset){
        if (instance == null){
            synchronized (KafkaServerConsumer.class){
                if (instance == null){
                    instance = new KafkaServerConsumer(topics,bootstrapServer,groupId,offset);
                }
            }
        }
        return instance;
    }

    private KafkaServerConsumer(Properties properties, Collection topics){
        latch = new CountDownLatch(1);
        consumerThread = new ConsumerThread(latch,properties,topics);
    }

    private KafkaServerConsumer(Collection topics,String bootstrapServer,String groupId,String offset){
        latch = new CountDownLatch(1);
        consumerThread = new ConsumerThread(latch,topics,bootstrapServer,groupId,offset);
    }

    public void registerToKafkaEvent(String topic , String key , Consumer<ConsumerRecord<String,String>> func){
        consumerThread.registerToEvent(topic,key,func);
    }
    public void registerToKafkaEvent(String topic,Consumer<ConsumerRecord<String,String>> func){
        consumerThread.registerToEvent(topic,null,func);
    }

    public void listen(){
        logger.info("start listening to kafka");
        runThread = new Thread(consumerThread);
        runThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            consumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.info("hook is active",e);
            }
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
