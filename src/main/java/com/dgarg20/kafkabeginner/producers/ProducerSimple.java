package com.dgarg20.kafkabeginner.producers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
/**
 * Created by deepanshu on 21/5/18.
 */
public class ProducerSimple {
    public static void main(String args[]){
        String topic = "";
        String key = "";
        String value = "";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093"); //List of kafka brokers
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);

        ProducerRecord<String, String> record = new ProducerRecord(topic,key,value);
        producer.send(record);
        producer.close();

        System.out.println("SimpleProducer Completed.");
    }
}
