package com.pwc.genome.database;

public class Logger {

    static boolean showConsole = true;

    public static void hide(){
        showConsole = false;

    }
    public static void show(){
        showConsole = false;

    }


    public static void log(String str){
        if (showConsole){
            System.out.println(str);
        }
    }

    public static void show(String str){
        System.out.println(str);
    }
}
