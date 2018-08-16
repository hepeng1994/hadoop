package com.cattle34.study.day03.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.testng.annotations.Test;


public class OrderDemo {
    public static void main(String[] args) throws Exception {
        //配置环境
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);

        //选择要运行的类
        job.setJarByClass(OrderDemo.class);

        //选怎要运行的map与reduce

        job.setMapperClass(MapperDemo.class);
        job.setReducerClass(ReducerDemo.class);

        //map输出类型与red输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBeen.class);
        job.setOutputKeyClass(OrderBeen.class);
        job.setOutputValueClass(NullWritable.class);
        FileSystem fs = FileSystem.get(entries);
        Path path = new Path("F:\\order\\output");
        boolean exists = fs.exists(path);
        if (exists) fs.delete(path,true);
        //输入与输出路径
        FileInputFormat.setInputPaths(job,new Path("F:\\order\\input"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\order\\output"));
        //检测程序是否运行成功
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);
    }
}
