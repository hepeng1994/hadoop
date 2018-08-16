package com.cattle34.study.Day05.mycombiner;
//减少了reduce压力,进reduce之前先运行他MyCombiner
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        /*进行聚合的操作*/
        /*map端输出的数据形式：    hello，1*/
        int allCount = 0;
        for (IntWritable value : values) {
            int count = value.get();
            allCount += count;
        }

        /*输出目标了*/
        context.write(key, new IntWritable(allCount));
    }
}

