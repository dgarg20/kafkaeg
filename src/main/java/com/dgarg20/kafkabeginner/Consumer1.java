package com.dgarg20.kafkabeginner;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.net.InetAddress;
import java.util.*;

/**
 * Created by deepanshu on 28/6/17.
 */


/**
 * Simple consumer with auto commit
 * @Topic only one topic used
 * properties for the consumer are given using Properties Object
 */
public class Consumer1 {
    public static void main(String [] args) {
        Properties props = new Properties();
        //list of kafka brokers
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group5");
        props.put("enable.auto.commit", "true");
        //props.put("auto.commit.interval.ms", "1000");
        //props.put("session.timeout.ms", "60000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("test"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",record.partition(),
                            record.offset(), record.key(), record.value());

            }
        }
        catch (WakeupException e){
            e.printStackTrace();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally{
            consumer.close();
        }
    }
}
