package com.dgarg20.kafkabeginner.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;

/**
 * Created by deepanshu on 21/5/18.
 */
public class ProducerSimpleWithError {
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
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.print("message sent to partion "+ metadata.partition() + " to topic " + metadata.topic() + " at time " + new Date(metadata.timestamp()) );
        }
        catch(Exception e){
            e.printStackTrace();
            System.out.println("Synchronous write failed");
        }
        finally {
            producer.close();
        }
        System.out.println("SimpleProducer with a synchronous call  Completed.");
    }
}