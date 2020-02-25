package com.pwc.genome.Consumers;

import com.pwc.genome.model.Resume;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.JSONSchema;

/*
manually
    bin/pulsar-client produce Resumes --messages "{\"title\":\"6667\"}"

 */
import java.util.Date;

public class ResumeConsumer {

    String topic = "Resumes" ;
    String subscriptionName = "Resumes-Subscription1";

    PulsarClient pulsarClient = null ;
    JSONSchema<Resume>  jsonSchema = JSONSchema.of(SchemaDefinition.<Resume>builder().withPojo(Resume.class).build());

    Consumer<Resume> consumer = null;
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
        subscriptionName = topic + "-Subscription";
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    public ResumeConsumer() {
        try {
            init() ;

            produceMessage();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void produceMessage() throws PulsarClientException{
            Resume res = new Resume();
            res.setTitle("title from schema");
            byte[] ms = jsonSchema.encode(res);

            Producer producer = pulsarClient.newProducer(jsonSchema)
                    .topic(topic)
                    .create();
            System.out.println("produce message: " + new String(ms));
            producer.send(res);

    }

    private void init () throws PulsarClientException {

        System.out.println("Topic: " + topic + "  Subscription: " + subscriptionName);
        pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
        consumer = pulsarClient.newConsumer(jsonSchema)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscribe();
    }

    public void listenAndProcess() throws PulsarClientException {
        int count = 0 ;
        System.out.println("Count value to start " + count) ;
        Message<Resume> msg = null;
        while (!(consumer.hasReachedEndOfTopic())) {
            msg = consumer.receive();

            long val = msg.getPublishTime();
            Date date=new Date(val);
            System.out.println("published: " + date.toString());
            //System.out.println("toString: " + msg.toString());
            //System.out.println("key: " + msg.getKey());

            try {
                Resume receivedMessage = msg.getValue();
                count++ ;
                System.out.println(count  + " :" + receivedMessage.getTitle());
                consumer.acknowledge(msg);
            } catch (Exception e){
                System.out.println("error processing :" + new String(msg.getData()));

                consumer.negativeAcknowledge(msg);
                //e.printStackTrace();
            }

        }
    }

    public void close()  {
        try {
            consumer.close();
            pulsarClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args ) {
        ResumeConsumer resumeConsumer = null ;
        try {
            resumeConsumer = new ResumeConsumer();
            resumeConsumer.listenAndProcess();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            resumeConsumer.close();
        }

    }
}

