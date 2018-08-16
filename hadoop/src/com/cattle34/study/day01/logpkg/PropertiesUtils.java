package com.cattle34.study.day01.logpkg;

import java.util.Properties;

public class PropertiesUtils {
    /*经典面试：单例设计模式*/

    public static Properties getProp()  throws Exception{
        Properties prop = new Properties();
        prop.load(PropertiesUtils.class.getClassLoader().getResourceAsStream("com/cattle34/study/day01/logpkg/demo.properties"));
        return prop;
    }


    public static void main(String[] args) throws Exception {
        Properties prop = PropertiesUtils.getProp();
        System.out.println(prop.getProperty("PREFIX"));
    }
}
