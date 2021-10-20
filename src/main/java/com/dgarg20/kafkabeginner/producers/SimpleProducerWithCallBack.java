package com.dgarg20.kafkabeginner.producers;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Created by deepanshu on 21/5/18.
 */
public class SimpleProducerWithCallBack {
    public static void main(String args[]) {
        String topic = "";
        String key = "";
        String value = "";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093"); //List of kafka brokers
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);

        ProducerRecord<String, String> record = new ProducerRecord(topic, key, value);
            producer.send(record, new MyProductCallBack());

        System.out.println("SimpleProducer with a synchronous call  Completed.");
    }
}

class MyProductCallBack implements Callback{

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e != null)
            System.out.println("Async failed with an exception");
        else
            System.out.println("Async call was success");
    }
}
