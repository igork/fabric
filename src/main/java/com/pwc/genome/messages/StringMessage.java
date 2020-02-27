package com.pwc.genome.messages;

import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;

public class StringMessage extends BaseMessage{
    String data;

    public StringMessage(String data){
        this.data = data;
    }

    @Override
    public String getData() {
        return data;
    }

    @Override
    public String getType() {
        return "String";
    }

    @Override
    public String toJson() {
        log("no json conversion needed for " + this.getType() + ": " + this.getData());
        return data;
    }

    @Override
    public StringMessage fromJson(String message){
        return fromString(message);
    }

    public static StringMessage fromString(String message){

        log(String.class.getSimpleName() + " is parsed: " + message);
        return new StringMessage(message);

    }
}
