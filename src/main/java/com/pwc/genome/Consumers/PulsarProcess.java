package com.pwc.genome.Consumers;

import com.pwc.genome.model.Resume;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.pulsar.shade.org.codehaus.jackson.map.util.JSONPObject;

import java.io.StringWriter;

/*
cd /Applications/
cd apache-pulsar-2.5.0/
bin/pulsar-client produce my_topic --messages "{\"title\":6666}"

 */

/*
schema
{"type":"record","name":"Resume","namespace":"com.pwc.genome.model","fields":[{"name":"title","type":["null","string"],"default":null}]}

 schema info
 {
  "name": "",
  "schema": {
    "type": "record",
    "name": "Resume",
    "namespace": "com.pwc.genome.model",
    "fields": [
      {
        "name": "title",
        "type": [
          "null",
          "string"
        ]
      }
    ]
  },
  "type": "JSON",
  "properties": {
    "__alwaysAllowNull": "true"
  }
}
 */

public class PulsarProcess {

    static PulsarClient client;
    static Consumer consumer;
    static Producer producer;

    static String topic = "my_topic";
    static String subscription = "my_subscr";

    public static void main(String[] args){

        try {

            init();

            producing();

            consuming();

            close();

        } catch (Exception e){
            e.printStackTrace();
        }


    }

    private static void init() throws Exception{
        client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();
        producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

    }

    private static void close() throws Exception{
        producer.close();
        consumer.close();
        client.close();

    }

    private static void consuming() throws Exception{

            while (true) {
                // Wait for a message
                Message msg = consumer.receive();

                try {
                    // Do something with the message
                    parsing(msg.getData());

                    // Acknowledge the message so that it can be deleted by the message broker
                    consumer.acknowledge(msg);
                } catch (Exception e) {
                    // Message failed to process, redeliver later
                    consumer.negativeAcknowledge(msg);
                }
            }




    }

    private static void producing() throws Exception{

        producer.send("My message");

        Resume res = new Resume();
        res.setTitle("produced title");

        producer.send(toJson(res));

    }

    private static String toJson(Resume res){
        ObjectMapper objectMapper = new ObjectMapper();

        //configure Object mapper for pretty print
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        StringWriter stringRes = new StringWriter();

        try {
            //writing to console, can write to any output stream such as file
            objectMapper.writeValue(stringRes, res);
            System.out.println("converting class to json: " + stringRes);
        } catch (Exception e){
            e.printStackTrace();
        }

        return stringRes.toString();

    }
    private static Object parsing( byte[] message ){

        return parsing(new String(message));
    }

    private static Object parsing( String message ){

        System.out.printf("Message received: %s\n", message);

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            //convert json string to object
            Resume res = objectMapper.readValue(message, Resume.class);

            System.out.println("Resume title parsed: " + res.getTitle());

            //convert Object to json string
            //Resume res2 = new Resume();
            //res2.setTitle("sample title");
            //toJson(res2);

            return res;
        } catch (Exception e) {

        }


        return null;
    }

}
