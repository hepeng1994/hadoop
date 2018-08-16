package com.cattle34.study.day02.mrpkg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, IntWritable,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //进行聚合操作
        int allCount =0;
        for (IntWritable value : values) {
            int count = value.get();
            allCount+=count;
        }
        //输出目标
        context.write(key,new IntWritable(allCount));
    }
}
