package com.nadav.kafka.server.consumer.Impl;

import com.nadav.kafka.server.consumer.Interfaces.IKafkaServerRegister;
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
    private IKafkaServerRegister kafkaServerRegister;

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
        kafkaServerRegister = new KafkaServerConsumerListRegister();
        consumerThread = new ConsumerThread(latch,properties,topics,kafkaServerRegister.getRegisterMap());

    }

    private KafkaServerConsumer(Collection topics,String bootstrapServer,String groupId,String offset){
        latch = new CountDownLatch(1);
        kafkaServerRegister = new KafkaServerConsumerListRegister();
        consumerThread = new ConsumerThread(latch,topics,bootstrapServer,groupId,offset,kafkaServerRegister.getRegisterMap());
    }

    public IKafkaServerRegister registerToKafkaEvent(String topic , String key , Consumer<ConsumerRecord<String,String>> func){
        return kafkaServerRegister.registerToKafkaEvent(topic,key,func);
    }

    public IKafkaServerRegister registerToKafkaEvent(String topic,Consumer<ConsumerRecord<String,String>> func){
        return kafkaServerRegister.registerToKafkaEvent(topic,null,func);
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
