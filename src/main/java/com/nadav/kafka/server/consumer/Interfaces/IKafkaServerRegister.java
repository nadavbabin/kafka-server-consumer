package com.nadav.kafka.server.consumer.Interfaces;

import com.nadav.kafka.server.consumer.Impl.KafkaServerConsumerListRegister;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.function.Consumer;

public interface IKafkaServerRegister {

    public KafkaServerConsumerListRegister registerToKafkaEvent(String topic , String key , Consumer<ConsumerRecord<String,String>> func);
    public KafkaServerConsumerListRegister registerToKafkaEvent(String topic,Consumer<ConsumerRecord<String,String>> func);
    public MultiKeyMap<String,List<Consumer<ConsumerRecord<String,String>>>> getRegisterMap();

}
