package com.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.example.model.FileContent;
import com.example.model.words;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;


public class KafkaSpark {
    static int primitiveCount =0;
    static int objectCount = 0;
    static int interfaceCount = 0;
    static int functionCount = 0;
    static int classCount = 0;
          public static boolean checkPrimitive(String s){
              return s.equals("int") || s.equals("boolean") || s.equals("byte") || s.equals("long") || s.equals("double") || s.equals("float") || s.equals("char") || s.equals("short");
          }
         public static int primitivesCountFunc(String s){
             int count =0;
             int index =-1;
             String[] words = s.split(" ");
             for(int i=0;i< words.length;i++){
                 if(checkPrimitive(words[i])){
                     index = i;
                     if(index !=0) {
                         String prev = words[index - 1];
                         char ch = prev.charAt(prev.length() - 1);
                         if (!(ch == '<')) {
                             count++;
                         }
                         else{
                             count++;
                         }
                     }
                     else{
                         count++;
                     }
                 }

             }

             return count;
         }
         public static int classCountFunc(String s){
             int count =0;
             String[] words = s.split("\\s+");
             for(String it :words){
                 if(it.equals("class")) {
                     count++;
                     break;
                 }
             }
             return count;
         }
         public static int objectCountFunc(String s){
             int count =0;
             String[] words = s.split("\\s+");
             for(String it :words){
                 if(it.equals("new")) {
                     count++;
                     break;
                 }
             }
             return count;

         }
         public static int interfaceCountFunc(String s){
             int count =0;
             String[] words = s.split("\\s+");
             for(String it :words){
                 if(it.equals("interface")) {
                     count++;
                     break;
                 }
             }
             return count;
         }
         public static int functionCountFunc(String s){
             int count =0;
             String[] words = s.split("\\s+");
             Integer i1=-1;
             Integer i2 =-1;
             for(int i=0;i< words.length;i++){
                 if(i1 == -1){
                     if(words[i].contains("(")){
                         i1 =i;
                     }
                 }
                 else if(i2 ==-1){
                     if(words[i].contains(")")){
                         i2 = i;
                     }
                 }
                 else{
                     if(words[i].contains("{") && (i-i2)==1){
                         count++;
                         break;
                     }
                 }
             }
             return count;
         }
         public static int getGlobalCounts(String s,Session session){
              String query = "select "+s+ " from kafka_table where uid=0d497e7b-a279-4653-a720-774615d6cb74;";
              System.out.println("global query:"+query);
              ResultSet new_count = session.execute(query);
              List<Integer> ls = new ArrayList<>();
              new_count.forEach(x->
                     ls.add(x.getInt(s))
             );

              if(ls.size()==0)
                  return 0;
             return ls.get(0);
         }
        public static String getFileType(String fileN){
            String fileType="";
            Integer i=0;
            while(fileN.charAt(i)!='.'){
                i++;
            }
            i++;
            for(Integer j=i;j<fileN.length();j++){
                fileType += fileN.charAt(j);
            }
            return fileType;
        }

        public static void main(String[] args) throws InterruptedException {
         System.setProperty("hadoop.home.dir", "C:\\winutils");

         Map<String, Object> kafkaParams = new HashMap<>();
         kafkaParams.put("bootstrap.servers", "localhost:9092");
         kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         kafkaParams.put("value.deserializer", "com.example.FileContentDeserializer");
         kafkaParams.put("group.id", "test1");
         kafkaParams.put("auto.offset.reset", "latest");
         kafkaParams.put("enable.auto.commit", false);

         SparkConf sparkConf = new SparkConf();
         sparkConf.setAppName("com.example.KafkaSpark");
         sparkConf.setMaster("local[2]");
         sparkConf.set("spark.cassandra.connection.host", "localhost");


         ArrayList<String> topics = new ArrayList<>();
         topics.add("kafka_consumer");
         JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(1000));
         JavaInputDStream<ConsumerRecord<String, FileContent>> messages = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, FileContent> Subscribe(topics, kafkaParams));
         JavaPairDStream<String, FileContent> results = messages.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
         JavaDStream<FileContent> lines = results.map(Tuple2::_2);


         JavaSparkContext ctx = ssc.sparkContext();
            lines.foreachRDD(
                 javaRdd-> {

                     List<FileContent> shs = javaRdd.collect();
                     shs.forEach(fileContent -> {
                         String fileN = fileContent.getFileName();
                         String[] contentArray = fileContent.getFileContent().split("\\r?\\n|\\r");
                         for (String s1 : contentArray) {
                             primitiveCount += primitivesCountFunc(s1);
                             objectCount += objectCountFunc(s1);
                             interfaceCount += interfaceCountFunc(s1);
                             functionCount += functionCountFunc(s1);
                             classCount += classCountFunc(s1);
                         }

                         String fileType = getFileType(fileN);
                         List<words> wordsList = Collections.singletonList(new words(UUID.randomUUID(), fileN, fileType,primitiveCount, objectCount, functionCount, classCount, interfaceCount));
                         JavaRDD<words> rdd1 = ctx.parallelize(wordsList);
                         javaFunctions(rdd1).writerBuilder(
                                 "vocabulary", "kafka_table", mapToRow(words.class)).saveToCassandra();


                     Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
                     Session session = cluster.connect("vocabulary");
                     int primC = getGlobalCounts("primitive_count",session)+primitiveCount;
                     int classC = getGlobalCounts("class_count",session)+classCount;
                     int functionC = getGlobalCounts("function_count",session)+functionCount;
                     int interfaceC = getGlobalCounts("interface_count",session)+interfaceCount;
                     int objectC = getGlobalCounts("object_count",session)+objectCount;
                     System.out.printf("%d %d %d %d %d :",primC,classC,functionC,interfaceC,objectC);
                     String query ="UPDATE kafka_table SET function_count="+functionC+", class_count="+classC+", object_count="+objectC+", interface_count="+interfaceC+", primitive_count="+primC+" WHERE uid=0d497e7b-a279-4653-a720-774615d6cb74;";
                     System.out.println("query :"+query);
                     session.execute(query);
                     });

                 }

         );


         ssc.start();
         ssc.awaitTermination();
        }
}

