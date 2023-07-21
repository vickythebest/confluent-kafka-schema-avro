package com.coolhand.kafka.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class Customer_Consumer {

    public static void main(String[] args) {
        String topic_name="customers";
        Properties properties=new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.144.101:9092");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule");
        properties.setProperty("sasl.machanism", "PLAIN");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"customerGroup");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("schema.registry.url","http://192.168.144.101:8081");

        KafkaConsumer<String,Customer> consumer = new KafkaConsumer<String,Customer>(properties);
        consumer.subscribe(Collections.singletonList(topic_name));

        while(true){
            ConsumerRecords<String,Customer> customerRecords = consumer.poll(3000);

            int recordCount=customerRecords.count();
            System.out.println("Receive : "+recordCount+" records");

            for(ConsumerRecord<String,Customer> record:customerRecords){
                System.out.println(record.value());

            }
        }


    }
}
