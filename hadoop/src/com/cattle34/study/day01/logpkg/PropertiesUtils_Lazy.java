package com.cattle34.study.day01.logpkg;

import java.util.Properties;

public class PropertiesUtils_Lazy {
    private static Properties prop = null;


    /*经典面试：单例设计模式*/
    /*双重否定的模式*/
    public static Properties getProp() throws Exception {
        /*A   B  */
        if (prop == null) {
            /*A   B  */
            synchronized ("sss") {
                if (prop == null) {
                    prop = new Properties();
                    prop.load(PropertiesUtils_Lazy.class.getClassLoader().getResourceAsStream("com/cattle34/study/day01/logpkg/demo.properties"));

                }

            }
        }


        return prop;
    }


    public static void main(String[] args) throws Exception {
        Properties prop = PropertiesUtils_Lazy.getProp();
        System.out.println(prop.getProperty("PREFIX"));
    }
}
