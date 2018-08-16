package com.cattle34.study.day01.logpkg;

import java.util.Properties;

public class PropertiesUtils_hungry {

    private static Properties prop = new Properties();

    static {
        try {
            prop.load(PropertiesUtils_hungry.class.getClassLoader().getResourceAsStream("com/cattle34/study/day01/logpkg/demo.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    /*经典面试：单例设计模式*/

    public static Properties  getProp()  throws Exception{
        return prop;
    }


    public static void main(String[] args) throws Exception {
        Properties prop = PropertiesUtils_hungry.getProp();
        System.out.println(prop.getProperty("PREFIX"));
    }
}
