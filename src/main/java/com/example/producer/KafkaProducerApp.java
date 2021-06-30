package com.example.producer;

import com.example.model.FileContent;
import org.apache.kafka.clients.producer.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;


public class KafkaProducerApp {

    public static void sendProducer(Producer<String ,FileContent> myProducer,ProducerRecord<String , FileContent> record){
        try {
            myProducer.send(record,
                    (recordMetadata, e) -> {
                        if (e != null) {
                            System.out.println("AsynchronousProducer failed with an exception");
                        } else {
                            System.out.printf("Topics: %s , Partition: %d,Key: %s%n",
                                    record.topic(), record.partition(), record.key());
                        }
                    });
        }catch(Exception e){
            e.printStackTrace();
        }

    }
    public static void displayFiles(File[] files,Producer<String,FileContent> myProducer) throws FileNotFoundException {
        for (File filename : files)
        {
            if (filename.isDirectory())
            {
                System.out.println("Directory: "
                        + filename);

                displayFiles(Objects.requireNonNull(filename.listFiles()),myProducer);
            }
            else
            {
                System.out.println("File: "
                        + filename.getName()+"\n");
                String fileContent = "";
                File file = new File(filename.getPath());
                Scanner sc = new Scanner(file);
                while(sc.hasNextLine()){
                    fileContent = fileContent.concat( sc.nextLine()+"\n");
                }
                FileContent fc = new FileContent();
                fc.setFileName(filename.getName());
                fc.setFileContent(fileContent);
                ProducerRecord<String ,FileContent> record = new ProducerRecord<>("kafka_consumer", UUID.randomUUID().toString(),fc);
                sendProducer(myProducer,record);

            }
        }
    }
    public static void main(String[] args) throws FileNotFoundException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","com.example.FileContentSerializer");

        Producer<String,FileContent> myProducer = new KafkaProducer<>(props);
        File[] files = new File("E:\\spring-framework-main\\spring-framework-main").listFiles();
        assert files != null;
        displayFiles(files,myProducer);
        myProducer .close();
    }
}
