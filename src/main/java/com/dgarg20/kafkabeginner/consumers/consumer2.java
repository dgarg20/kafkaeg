package com.dgarg20.kafkabeginner.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
/**
 * Created by deepanshu on 28/6/17.
 */


/**
 *
 * @topic only one
 * no auto commit offset is set
 *@consumer.commitSync() can also accept parameters if want to set some other offset or metadata
 * commitSync(Map(<partition>,OffsetAndMetadata)
 * if no argument is passed it commits the last message offset that is read
 *
 * commitsync is blocking till the commit fails or succeeds
 *
 * worst case message repeated = the number of messages your application can process during the
 *  commit interval (as configured by auto.commit.interval.ms).
 */
public class consumer2 {
    public static void main(String [] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group5");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        //props.put("session.timeout.ms", "60000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("test2"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                //System.out.print(records);
                try {
                    //use commit here if u want that any time current offset should not cross last
                    // commited offset
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    /*consumer.commitSync(Collections.singletonMap(record.partition(),
                            new OffsetAndMetadata(record.offset() + 1)));
                    */
                   // consumer.commitSync();
                    }
                }
                catch (CommitFailedException e) {
                    e.printStackTrace();
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
