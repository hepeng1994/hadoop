package com.cattle34.study.Day05.MRquchong;

import com.cattle34.study.day03.lineAnLi;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class QuChong {
    /*
    去重思想利用mapreduce 中 的思想;就是进入reduce之前要分组,把要去重的元素设在Map为KEY输出
     */
    public static class QuChongMap extends Mapper<LongWritable,Text,Text,NullWritable>{
        //把new的东西提出来防止一直new东西
        Text car =new Text();
        NullWritable nullWritable=NullWritable.get();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String []split = value.toString().split(",");
            car.set(split[0]);
            context.write(car,nullWritable);
        }
    }
    public static class QuChongReduce extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Path path1 = new Path("E:\\x\\Hadoop\\新hadoop\\day04\\mrdata\\car-no\\");
        Path in = new Path(path1 + "\\input");
        Path out = new Path(path1 + "\\output");
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);
        //要运行的程序
        job.setJarByClass(QuChong.class);
        //要运行的mapper与reduce
        job.setMapperClass(QuChongMap.class);
        job.setReducerClass(QuChongReduce.class);
        //map与reduce输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //输入与输出路径
        FileSystem fs = FileSystem.get(entries);
        boolean exists = fs.exists(out);
        if (exists) fs.delete(out,true);


        FileInputFormat.setInputPaths(job,in );
        FileOutputFormat.setOutputPath(job, out);

        //程序监控
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);
    }
}
