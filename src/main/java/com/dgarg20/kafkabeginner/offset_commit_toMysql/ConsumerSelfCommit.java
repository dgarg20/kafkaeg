package com.dgarg20.kafkabeginner.offset_commit_toMysql;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;


import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by deepanshu on 22/5/18.
 */
public class ConsumerSelfCommit {
    public static void main(String args[]){
        String topicName = "test";
        Properties props =new Properties();

        props.put("bootstrap.server", "localhost:9092, localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("enable.auto.commit",false);

        KafkaConsumer<String, String> consumer =new KafkaConsumer<String, String>(props);
        TopicPartition p0 = new TopicPartition(topicName, 0);
        TopicPartition p1 = new TopicPartition(topicName, 1);
        TopicPartition p2 = new TopicPartition(topicName, 2);

        consumer.assign(Arrays.asList(p0,p1,p2));


        consumer.seek(p0, getOffsetFromDB(p0));
        consumer.seek(p1, getOffsetFromDB(p1));
        consumer.seek(p2, getOffsetFromDB(p2));

        System.out.print("Starting consumer");

        try{
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(100);
                System.out.println("Record fetched = " + records.count());
                for(ConsumerRecord r: records){
                    System.out.println("Record from partition " + r.partition() + " " +
                            "with Key "+ r.key() + " and value " + r.value());
                    saveAndCommit(consumer, r);
                }
            }
        }
        catch(Exception e){
            System.out.print("Some exception occured");
            e.printStackTrace();
        }
        finally{
            consumer.close();
        }

    }


    private static long getOffsetFromDB(TopicPartition p){
        long offset = 0;
        try{
            Class.forName("com.mysql.jdbc.Driver");
            Connection con= DriverManager.getConnection("jdbc:mysql://localhost:3306/kafka_learning","root","yatra@123");
            Statement stmt = con.createStatement();

            String sql = "select offset from topic_partition_offset where topic='" + p.topic() + "' and partition=" + p.partition();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next())
                offset = rs.getInt("offset");
            stmt.close();
            con.close();
        }catch(Exception e){
            System.out.println("Exception in getOffsetFromDB");
        }
        return offset;
    }

    private static void saveAndCommit(KafkaConsumer<String, String> c, ConsumerRecord<String, String> r){
        System.out.println("Topic=" + r.topic() + " Partition=" + r.partition() + " Offset=" + r.offset() + " Key=" + r.key() + " Value=" + r.value());
        try{
            Class.forName("com.mysql.jdbc.Driver");
            Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","pandey");
            con.setAutoCommit(false);
            String insertSQL = "insert into record values(?,?)";
            PreparedStatement psInsert = con.prepareStatement(insertSQL);
            psInsert.setString(1, r.key());
            psInsert.setString(2, r.value());

            String updateSQL = "update topic_partition_offset set offset=? where topic_name=? and partition=?";
            PreparedStatement psUpdate = con.prepareStatement(updateSQL);
            psUpdate.setLong(1,r.offset()+1);
            psUpdate.setString(2,r.topic());
            psUpdate.setInt(3,r.partition());

            psInsert.executeUpdate();
            psUpdate.executeUpdate();
            con.commit();
            con.close();
        }
        catch(Exception e){
            System.out.println("Exception in saveAndCommit");
        }
    }
}
