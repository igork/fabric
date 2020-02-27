package com.pwc.genome.Consumers;

import com.pwc.genome.database.Logger;
import com.pwc.genome.database.MongoServer;
import com.pwc.genome.messages.BaseMessage;
import com.pwc.genome.messages.ResumeMessage;
import com.pwc.genome.messages.SomeMessage;
import com.pwc.genome.messages.StringMessage;
import com.pwc.genome.model.Resume;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.SerializationFeature;

import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/*
cd /Applications/
cd apache-pulsar-2.5.0/
bin/pulsar-client produce my_topic --messages "{\"title\":6666}"

the following message will stop consuming()
bin/pulsar-client produce my_topic --messages "stop"

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

    static private PulsarClient client = null;
    static private Consumer<byte[]> consumer = null;
    static private Producer<String> producer = null;

    final static String topic = "my_topic";
    final static String subscription = "my_subscr";
    final static String serviceUrl = "pulsar://localhost:6650";

    static Date dt0 = null;
    static int steps = 0;

    static boolean isSavingInThread = true;
    static String[] admin = {"stop","more","load"};

    public static void pulsarTest() throws Exception{
        Logger.show("Pulsar consumer starts... " + dt0);

        init();

        producing();

        consuming();

        close();

    }


    public static void loadTest() throws Exception{

        Logger.show("Load & performance starts... " + dt0);
        if (steps>1)
            Logger.show("steps: " + steps);


        Logger.hide();

        init();

        for( int i=0; i<steps; i++) {
            try {

                producing();

                produceStop();

                consuming();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Logger.show();

        close();

        Logger.show();

    }

    public static void main(String[] args) throws Exception{

        if (args!=null && args.length>0 && args[0].equalsIgnoreCase("load")){
            steps = 1;
            if (args.length>1){
                try {
                    int iper = Integer.parseInt(args[1]);
                    if (iper>0)
                        steps = iper;
                } catch (Exception e){
                    Logger.show("cannot parse " + args[1] + " to in");
                }
            }

            loadTest();

        } else {

            pulsarTest();

        }


    }

    private static void init() throws Exception{
        client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();
        producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        MongoServer.init();

        //start timing
        dt0 = new Date();

    }

    private static void close() throws Exception{

        //show the final time log
        Logger.show();

        Date dt99 = new Date();
        long t = Math.abs(dt0.getTime() - dt99.getTime());

        Logger.show("Running time:  " + t );
        if (steps>1)
            Logger.show("Average step:" + (int) ((float) t / steps) );


        //closing servers
        producer.close();
        consumer.close();
        client.close();
        MongoServer.close();

        producer=null;
        consumer=null;
        client=null;

    }

    private static void consuming() throws Exception{

        boolean timeToStop = false;

        while (!timeToStop) {
                // Wait for a message
                Message<byte[]> msg = consumer.receive();

                //try {
                    // Do something with the message
                    BaseMessage result = parsing(msg.getData());

                    // Acknowledge the message so that it can be deleted by the message broker
                    consumer.acknowledge(msg);

                    saving(result);

                    if (wantProduceMore(result)){
                        producing();
                    }

                    timeToStop = timeToStop(result);

                //} catch (Exception e) {
                    // Message failed to process, redeliver later
                    //consumer.negativeAcknowledge(msg);
                //}
            }

    }

    private static void produceStop() throws Exception {

        //string
        producer.send(new StringMessage("stop").toJson());
    }

    private static void producing() throws Exception{

        //bytes
        byte[] b = new byte[] { 97, 98, 98, 0, 11};
        producer.send(new BaseMessage(b).toJson());

        //string
        producer.send( new StringMessage("My message").toJson());

        //resume
        Resume res = new Resume();
        res.setTitle("produced resume title");
        ResumeMessage rm = new ResumeMessage(res);
        producer.send(rm.toJson());

        //something else
        SomeMessage sm = new SomeMessage("some data");
        producer.send(sm.toJson());


    }

    private static BaseMessage parsing( byte[] message ){

        return parsing(new String(message));
    }

    private static boolean wantProduceMore(Object object){
        if (object==null)
            return false;

        if (!(object instanceof StringMessage))
            return false;

        StringMessage msg = (StringMessage)object;
        String text = msg.getData();

        if (text==null)
            return false;

        return text.equalsIgnoreCase("more");
    }

    private static boolean timeToStop(Object object){
        if (object==null)
            return false;

        if (!(object instanceof StringMessage))
            return false;

        StringMessage msg = (StringMessage)object;
        String text = msg.getData();

        if (text==null)
            return false;

        return text.equalsIgnoreCase("stop");
    }
    private static boolean startLoadTest(Object object){
        return false;
    }

    private static BaseMessage parsing( String message ) {

        Logger.log("\nMessage received: " + message + "\n");

        BaseMessage some = null;
        if (BaseMessage.isJSONValid(message)){
            some = ResumeMessage.fromString(message);
            if (some==null){
                some = SomeMessage.fromString(message);
            }
        } else {

            if (BaseMessage.isStringOfBytes(message)){
                some = BaseMessage.fromString(message);
            }

        }
        if (some==null){
            some = StringMessage.fromString(message);
        }

        return some;


    }

    private static boolean isAdminFunction(Object msg){

        return timeToStop(msg) || wantProduceMore(msg) || startLoadTest(msg);
    }
    public static void saving(BaseMessage message){

        //do not save admin messages
        if (isAdminFunction(message)){
            return;
        }

        Map<String,String> map = new HashMap<String,String>();
        map.put("type",message.getType());
        map.put("content",message.toJson());
        map.put("userid","pulsar");

        if (isSavingInThread)
            MongoServer.saveInThread(map);
        else
            MongoServer.save(map);
    }


}
