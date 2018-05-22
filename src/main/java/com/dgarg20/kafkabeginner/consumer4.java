package com.dgarg20.kafkabeginner;

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
 * Created by deepanshu on 29/6/17.
 */

/**
 *
 * Using commitAsync in consumer and also the
 *
 */

public class consumer4 {
        public static void main(String [] args) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "group22");
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
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionsRecords = records.records(partition);
                        for (ConsumerRecord<String, String> record : partitionsRecords) {
                            System.out.println(record.offset() + ": " + record.key() + "-> "
                                    + record.value());

                            //    long lastoffset = partitionsRecords.get(partitionsRecords.size() - 1).offset();
                            //              consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastoffset + 1)));}
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

