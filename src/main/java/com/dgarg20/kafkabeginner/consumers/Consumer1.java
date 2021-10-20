 package com.dgarg20.kafkabeginner.consumers;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;



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
        props.put("bootstrap.servers", "10.50.82.112:6667");
        props.put("group.id", "drainout");
        props.put("enable.auto.commit", "true");
        //props.put("auto.commit.interval.ms", "1000");
        //props.put("session.timeout.ms", "60000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("lotIncoming"));
        consumer.poll(10);
        consumer.
        consumer.seekToBeginning(consumer.assignment());
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(10);
                for (ConsumerRecord<String, String> record : records)
                    if(record.offset() > 150)
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
