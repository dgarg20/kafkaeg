package com.dgarg20.kafkabeginner;

import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Created by deepanshu on 21/5/18.
 */
public class ProducerWithCustomSerializer {
    public static void main(String args[]) {
        String topic = "";
        String key = "";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093"); //List of kafka brokers
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "SongSerializer");
        SimpleDateFormat df = new SimpleDateFormat("dd-MM-yy");
        try {
            Song s = new Song("Shape Of You", "Ed shreen ", df.parse("01-01-2010"), 1);
            Producer<String, Song> producer = new KafkaProducer<String, Song>(props);

            producer.send(new ProducerRecord<String, Song>(topic, key, s));
        }
        catch(Exception e){
            System.out.print("Exception Occured");
            e.printStackTrace();
        }
    }
}

class Song{
    private String name;
    private String singer;
    private Date releaseDate;
    private int songId;

    public Song(String name, String singer, Date releaseDate, int id){
        this.name = name;
        this.singer = singer;
        this.releaseDate = releaseDate;
        this.songId = id;
    }

    public int getSongId(){
        return songId;
    }

    public String getName() {
        return name;
    }

    public Date getReleaseDate(){
        return releaseDate;
    }

    public String getSinger(){
        return singer;
    }
    @Override
    public String toString(){
        return "id " + songId + " name " + name + " singer "+ singer + " release date " + releaseDate.toString();
    }
}


class SongSerializer implements Serializer<Song> {



    public void configure(Map<String, ?> map, boolean b) {
        //No need of configuration parameters
    }

    public byte[] serialize(String topic, Song song) {
        if(song == null) {
            System.out.println("No data about the song recieved");
            return null;
        }
        try{
            byte[] serializedName = song.getName().getBytes("UTF-8");
            int sizeName = serializedName.length;

            byte[] serializedSinger = song.getSinger().getBytes("UTF-8");
            int sizeSinger = serializedSinger.length;

            byte[] serializedReleaseDate = song.getReleaseDate().toString().getBytes("UTF-8");
            int sizeReleaseDate = serializedReleaseDate.length;


            ByteBuffer buf = ByteBuffer.allocate(4 + sizeName + sizeSinger + sizeReleaseDate);
            buf.putInt(song.getSongId());

            buf.putInt(sizeName);
            buf.put(serializedName);

            buf.putInt(sizeSinger);
            buf.put(serializedSinger);

            buf.putInt(sizeReleaseDate);
            buf.put(serializedReleaseDate);

            return buf.array();
        }
        catch (Exception e){
            System.out.println("Some error occured");
            e.printStackTrace();
            throw new SerializationException("Unable to serialize the given Song");
        }
    }

    public void close() {
        //NOTHING TO DO
    }
}


class SongDeserializer implements Deserializer<Song>{

    public void configure(Map<String, ?> map, boolean b) {

    }

    public Song deserialize(String topic, byte[] bytes) {
        if(bytes == null)
            return null;
        try{
            ByteBuffer buff = ByteBuffer.wrap(bytes);
            int id = buff.getInt();

            int sizeName = buff.getInt();
            byte[] nameByte = new byte[sizeName];
            buff.get(nameByte);
            String name = new String(nameByte, "UTF-8");

            int sizeSinger = buff.getInt();
            byte[] singerByte = new byte[sizeSinger];
            buff.get(singerByte);
            String singer = new String(singerByte, "UTF-8");

            int sizeDate = buff.getInt();
            byte[] dateByte = new byte[sizeDate];
            buff.get(dateByte);
            Date releaseDate = new Date(new String(dateByte, "UTF-8"));

            Song s = new Song(name, singer, releaseDate, id);

            System.out.print(s.toString());

            return s;
        }
        catch(Exception e)
        {
            System.out.println("unable to deserialize");
            throw new DeserializationException("unable to deserialize the bytes to song object");
        }
    }

    public void close() {
        //nothing to be done
    }
}