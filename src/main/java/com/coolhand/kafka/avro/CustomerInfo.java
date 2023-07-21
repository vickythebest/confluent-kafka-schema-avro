package com.coolhand.kafka.avro;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomerInfo
{
    public static void main( String[] args )
    {

        String topic_name="customers";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.144.101:9092");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule");
        properties.setProperty("sasl.machanism", "PLAIN");

        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule");
        properties.setProperty("sasl.machanism", "PLAIN");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        properties.put("schema.registry.url","http://192.168.144.101:8081");

        KafkaProducer<String, Customer> producer= new KafkaProducer<String,Customer>(properties);

        Customer customer = Customer.newBuilder()
                .setFirstName("Customner")
                .setLastName("1")
                .setAge(36)
                .setHeight(157)
                .setWeight(160)
                .build();

        ProducerRecord record=new ProducerRecord<String,Customer>(topic_name,customer);

        System.out.println("customer = " + customer);

        producer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null){
                    System.out.println("Success !! "+recordMetadata.toString());
//                    System.out.println(recordMetadata.toString());
                }else{
                    System.out.println("Exception");
                    e.printStackTrace();
                }
            }

        });
        System.out.println("Flush producer");
        producer.flush();
        System.out.println("Close producer");
        producer.close();
    }
}
