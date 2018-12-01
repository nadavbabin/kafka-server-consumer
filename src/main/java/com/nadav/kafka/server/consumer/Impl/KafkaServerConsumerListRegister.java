package com.nadav.kafka.server.consumer.Impl;

import com.nadav.kafka.server.consumer.Interfaces.IKafkaServerRegister;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class KafkaServerConsumerListRegister implements IKafkaServerRegister {

    private MultiKeyMap<String,List<Consumer<ConsumerRecord<String,String>>>> registerMap;
    private static final int initialCapacity = 2;
    private final String sync = "sync";

    public KafkaServerConsumerListRegister(){
        registerMap = new MultiKeyMap<>();
    }


    private boolean checkExist(String topic,String key){
           return registerMap.get(topic,key) != null;
    }

    private void initRowInMap(String topic,String key){
        registerMap.put(topic,key,new ArrayList<Consumer<ConsumerRecord<String,String>>>(initialCapacity));
    }

    private KafkaServerConsumerListRegister register(String topic, String key, Consumer<ConsumerRecord<String, String>> func){
        synchronized (sync){
            boolean rowExist = checkExist(topic,key);
            if (!rowExist){
                initRowInMap(topic,key);
            }
            registerMap.get(topic,key).add(func);
        }
        return this;
    }

    @Override
    public KafkaServerConsumerListRegister registerToKafkaEvent(String topic, String key, Consumer<ConsumerRecord<String, String>> func) {
        return register(topic,key,func);
    }

    @Override
    public KafkaServerConsumerListRegister registerToKafkaEvent(String topic, Consumer<ConsumerRecord<String, String>> func) {
        return register(topic,null,func);
    }

    public MultiKeyMap<String,List<Consumer<ConsumerRecord<String,String>>>> getRegisterMap(){
        return this.registerMap;
    }
}
