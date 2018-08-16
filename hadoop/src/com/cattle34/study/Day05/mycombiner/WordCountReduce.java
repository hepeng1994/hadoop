package com.cattle34.study.Day05.mycombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * reducer类，同样接受两组参数
 * 第一组：map端的输出
 * key：map端输出的key：   ---  word
 * value:map端输出的value    ---- 1
 * 第二组：最终的结果输出
 * key   reduce端输出的key   ---- word
 * value  reduce端输出的value   ---- 6 （word总共有6个）
 *
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     *
     * @param key   我们的word单词
     * @param values    代表返回该key下的所有  值：   （ 1， 1， 1， 1， 1， ....)
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
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
