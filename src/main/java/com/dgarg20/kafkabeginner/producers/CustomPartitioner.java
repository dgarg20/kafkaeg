package com.dgarg20.kafkabeginner.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by deepanshu on 21/5/18.
 */

/**
 * To send data of sensor s1 to 30% of the starting partitions and
 * data from rest of the sensors to other partitions
 */



public class CustomPartitioner {
    public static void main(String args[]) {
        String topic = "";
        String key = "";
        String value = "";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093"); //List of kafka brokers
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partition.class", "Mypartition");
        props.put("sensor.name" , "S1");
        Producer<String, String> producer = new KafkaProducer(props);
        for(int i = 0 ; i < 10; i++ ) {
            //ProducerRecord<String, String> record = new ProducerRecord(topic, key, value);
            producer.send(new ProducerRecord<String, String>(topic, "ssp" + i, "500" + i));
        }
        for(int i = 0 ; i < 10; i++ ) {
            //ProducerRecord<String, String> record = new ProducerRecord(topic, key, value);
            producer.send(new ProducerRecord<String, String>(topic, "S1", "500" + i));
        }
        System.out.println("SimpleProducer with custom partitioner completed");
    }
}

class MyPartition implements Partitioner{

    private String sensorName;
    public int partition(String topic, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        List<PartitionInfo> partition = cluster.partitionsForTopic(topic);
        int numPartitions = partition.size();
        int sp = (int)Math.abs(.3* numPartitions);
        int p;
        if(bytes == null || !(o instanceof System))
            throw new InvalidRecordException("Messages should have a name");
        if( ((String)o).equals(sensorName) )
            //Because if we hash Key the key will equal to sensor name and hence we will get
            //same partition every time so the load will not be distributed
            p = Utils.toPositive(Utils.murmur2(bytes1)) % sp;
        else
            //So that each other data which is not sensor name will go to other sever
            // partition equally and split
            p = Utils.toPositive(Utils.murmur2(bytes)) % (numPartitions - sp) + sp;
        System.out.print("Key " + o1 + " Assigned to Partiton " + p);

        return p;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {
        sensorName = map.get("sensor.name").toString();
    }
}
