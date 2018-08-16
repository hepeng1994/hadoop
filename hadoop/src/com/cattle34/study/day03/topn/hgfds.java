package com.cattle34.study.day03.topn;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class hgfds {
    public static void main(String[] args) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader("F:\\order\\input\\order.txt"));
        String str;
        String[] split=null;
        while((str=br.readLine())!=null){
            try{
            split = str.split(",");
            System.out.println(split[0]);
            System.out.println(split[1]);
            System.out.println(split[2]);
            System.out.println(split[3]);
            System.out.println(split[4]);
            System.out.println(split[5]);
            }catch (Exception e){
               System.out.println(str);
            }


        }
    }
}
