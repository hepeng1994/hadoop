package com.cattle34.study.day03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.File;
import java.io.IOException;
import java.net.URI;


public class lineAnLi {
    public static class LineMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            for (int i = Integer.parseInt(split[0]); i <= Integer.parseInt(split[1]); i++) {
                context.write(new IntWritable(i), new IntWritable(1));
            }
        }
    }

    public static class LineReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int allCount = 0;
            for (IntWritable value : values) {
                int i = value.get();
                allCount += i;
            }
            context.write(key, new IntWritable(allCount));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);
        //要运行的程序
        job.setJarByClass(lineAnLi.class);
        //要运行的mapper与reduce
        job.setMapperClass(LineMap.class);
        job.setReducerClass(LineReduce.class);
        //map与reduce输出类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        //输入与输出路径
        FileSystem fs = FileSystem.get(entries);
        Path path = new Path("E:\\x\\Hadoop\\cdc\\output\\");
        boolean exists = fs.exists(path);
        if (exists) fs.delete(path,true);


        FileInputFormat.setInputPaths(job, new Path("E:\\x\\Hadoop\\cdc\\input\\"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\x\\Hadoop\\cdc\\output\\"));

        //程序监控
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);
    }
}
