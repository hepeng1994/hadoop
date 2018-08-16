package com.cattle34.study.Day05.mycombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * Serializable:java中的，比较臃肿，进行网络开销时，效率较低
 * hadoop开发了自己的序列化的方式,在hadoop集群中传输效率较高
 * long --> LongWritable
 * int ---> IntWritable
 * String ---> Text
 * null ---> NullWritable
 */

/**
 * 在mapper类中：接受2组参数
 * 第一组：
 * 输入的key，
 * 输入的value
 * 第二组：
 * map端输出的key
 * map端输出的value
 */
/*需求是：统计wordcount的mapreduce版本*/
/*我们的输出： hello    1   */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*得到的输入的value就是 每一行的数据：*/
        /*hello java   hello scala  hello hadoop*/
        String[] words = value.toString().split(" ");
        /*目标：  输出  word   1 这种形式*/
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
