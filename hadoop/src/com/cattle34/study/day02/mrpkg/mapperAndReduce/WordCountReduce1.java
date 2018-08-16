package com.cattle34.study.day02.mrpkg.mapperAndReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce1 extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int allCount=0;
        for (IntWritable value : values) {
            int cunnt = value.get();
            allCount+=cunnt;
        }
        context.write(key,new IntWritable(allCount));
    }
}
