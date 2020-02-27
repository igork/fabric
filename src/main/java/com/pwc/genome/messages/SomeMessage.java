package com.pwc.genome.messages;

import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;


public class SomeMessage extends BaseMessage {

    SomeData data;
    public SomeMessage(String data){
        this.data = new SomeData();
        this.data.data1 = data;
    }
    @Override
    public SomeData getData() {
        return data;
    }

    @Override
    public String getType() {
        return "SomeData";
    }

    @Override
    public String toJson() {
        //TO DO: dirty fix
        //bytes = data.data1;
        return super.toJson(this);
    }

    @Override
    public SomeMessage fromJson(String message) {
        return fromString(message);
    }

    public static SomeMessage fromString(String message) {

        ObjectMapper objectMapper = new ObjectMapper();
        try {

            SomeData res = objectMapper.readValue(message, SomeData.class);
            log(SomeData.class.getSimpleName() + " is parsed: " + res.data1);
            return new SomeMessage(res.data1);
        } catch (Exception e) {
            //
            //System.out.println("---Error parsing " + getType() + " : " + e.getMessage());

        }
        return null;
    }
}
