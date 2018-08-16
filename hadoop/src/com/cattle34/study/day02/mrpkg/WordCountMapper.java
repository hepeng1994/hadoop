package com.cattle34.study.day02.mrpkg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //value就是每一行数据
        String[] split = value.toString().split(" ");
        for (String s : split) {
            context.write(new Text(s), new IntWritable(1));

        }
    }
}
