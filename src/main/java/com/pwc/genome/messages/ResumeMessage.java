package com.pwc.genome.messages;

import com.pwc.genome.model.Resume;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;

public class ResumeMessage extends BaseMessage{
    Resume res;

    public ResumeMessage(Resume res){
        this.res = res;
    }

    @Override
    public Resume getData() {
        return res;
    }

    @Override
    public String getType() {
        return "Resume";
    }

    @Override
    public String toJson() {
        return super.toJson(this);
    }

    @Override
    public ResumeMessage fromJson(String message){

        return fromString(message);
/*
        ObjectMapper objectMapper = new ObjectMapper();
        try {

            Resume res = objectMapper.readValue(message, Resume.class);
            System.out.println(getType() + " is parsed: " + res.getTitle());
            return new ResumeMessage(res);
        } catch (Exception e) {
            //
            //System.out.println("---Error parsing " + getType() + " : " + e.getMessage());

        }
        return null;
        */

    }

    public static ResumeMessage fromString(String message){
        try {
            Resume res = objectMapper.readValue(message, Resume.class);
            log(res.getClass().getSimpleName() + " is parsed: " + res.getTitle());
            return new ResumeMessage(res);
        } catch (Exception e) {
            //
            //System.out.println("---Error parsing " + getType() + " : " + e.getMessage());

        }
        return null;
    }
}
