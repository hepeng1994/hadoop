package com.cattle34.study.day02.mrpkg;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;

public class WCDemo {
    public static void main(String[] args) throws Exception {
        //加载配置文件
        Configuration conf = new Configuration();
        //创建Job

        Job job = Job.getInstance(conf);

        job.setJarByClass(WCDemo.class);
        //输入map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //输入reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //输入要运行的mapper与reduce程序
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //输入要原始文件与结果文件 的目录
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        //job.submit();火箭助推器 提交完成后,程序终止
        //卫星和地面控制中心,程序完成后返回Boolean值可判断程序运行状况
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);
    }
}
