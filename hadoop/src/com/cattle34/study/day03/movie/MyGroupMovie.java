package com.cattle34.study.day03.movie;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 */
public class MyGroupMovie extends WritableComparator {
    //防止空指针 在WritableComparator 必须输入参数
    public MyGroupMovie(){
        super(MovieBeen.class,true);
    }
//识别相同电影输入reduce
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MovieBeen a1 = (MovieBeen) a;
        MovieBeen b1 = (MovieBeen) b;
        return a1.getMovie().compareTo(b1.getMovie());
    }
}
