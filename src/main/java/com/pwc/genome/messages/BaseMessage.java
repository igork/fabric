package com.pwc.genome.messages;

import com.pwc.genome.database.Logger;
import com.pwc.genome.model.Resume;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

public class BaseMessage {
    static ObjectMapper objectMapper = new ObjectMapper();

    String bytes;

    public Object getData() {
        return bytes;
    }

    public BaseMessage(){

    }
    public BaseMessage(String bytes){
        this.bytes = bytes;
    }

    public BaseMessage(byte[] bytes) {
        this.bytes = Arrays.toString(bytes);
    }

    public String getType(){
        return "array of bytes";
    }

    public static void log(String str){
        Logger.log(str);
    }
    public String toJson(){
        log("no json conversion needed for " + this.getType() + ": " + this.getData());
        return bytes;
    }

    private static final Pattern HEXADECIMAL_PATTERN = compile("\\p{XDigit}+");
    private static boolean isHexadecimal(String input) {
        final Matcher matcher = HEXADECIMAL_PATTERN.matcher(input);
        return matcher.matches();
    }

    public static boolean isStringOfBytes(String message){


        if (message!=null && message.length()>=2){
            //remove double quota
            message = message.replaceAll("^\"|\"$", "");

            //System.out.println("first: " +message.charAt(0) + "  last: " + message.charAt(message.length()-1));


            if (message.charAt(0)=='[' && message.charAt(message.length()-1)==']'){
                //TO DO
                //check that all value separated by comma is 8-bits number
                return true;
            }

            //check HEX string
            return isHexadecimal(message);
        }

        return false;
    }
    public BaseMessage fromJson(String message){
        return BaseMessage.fromString(message);
    }
    public static BaseMessage fromString(String message){
        log(BaseMessage.class.getSimpleName() + " is parsed: " + message);
        return new BaseMessage(message);
    }

    protected static String toJson(BaseMessage message){

        //configure Object mapper for pretty print
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        StringWriter stringRes = new StringWriter();

        try {
            //writing to console, can write to any output stream such as file
            objectMapper.writeValue(stringRes, message.getData());
            //log
            log("converting " + message.getType() + " to: " + stringRes);
        } catch (Exception e){
            e.printStackTrace();
        }

        return stringRes.toString();

    }

    public static boolean isJSONValid(String json) {

        boolean valid = false;

        if(isStringOfBytes(json)){
            return valid;
        }

        try{
            JsonNode jn = objectMapper.readTree(json);

            final JsonParser parser = new ObjectMapper().getFactory()
                    .createParser(json);

            while (parser.nextToken() != null) {
            }
            valid = true;
        } catch(JsonProcessingException e){

        } catch (IOException e) {

        }
        return valid;
    }

}
