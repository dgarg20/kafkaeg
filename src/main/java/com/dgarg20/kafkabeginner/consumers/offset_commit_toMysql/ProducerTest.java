package com.dgarg20.kafkabeginner.consumers.offset_commit_toMysql;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.DataInputStream;
import java.util.Properties;

/**
 * Created by deepanshu on 22/5/18.
 */
public class ProducerTest {
    public static void main(String args[]){
        String topic = "test";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093"); //List of kafka brokers
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);

        System.out.println("Always enter key and value seprated by , ");
        try{
            DataInputStream ds = new DataInputStream(System.in);
            int p = 0;
            while(true) {

                String[] s = ds.readLine().split(",");
                producer.send(new ProducerRecord<String, String>(topic, p%3, s[0], s[1]));
                p++;
            }
        }
        catch(Exception e){
            System.out.println("error in producer");
        }
        finally {
            producer.close();
        }
        System.out.println("SimpleProducer Completed.");
    }
}
