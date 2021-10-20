package com.dgarg20.kafkabeginner.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by deepanshu on 28/6/17.
 */


/**
 * Go through consumer 1 and consumer 2 to understand
 *
 * in this commitSync is done after reading messages from one partition
 * consumerRecord gives access to records partition
 * iterate over each TopicPartition
 * each partition consumer records are read and then commited at the last
 * the no of messages that are read and process of partition is followed according to the poll interval
 *
 */
public class Consumer3 {
    public static void main(String [] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group10");
        props.put("enable.auto.commit", "false");
        //props.put("auto.commit.interval.ms", "1000");
        //props.put("session.timeout.ms", "60000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("test2"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for(TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionsRecords = records.records(partition);
                    for(ConsumerRecord<String, String> record : partitionsRecords ){
                        System.out.println(record.offset() + ": " + record.value());

                        long lastoffset = partitionsRecords.get(partitionsRecords.size() - 1).offset();
                        //  consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastoffset + 1)));
                    }
                }
            }
        }
        catch (WakeupException e){
            //ignoring the case
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
