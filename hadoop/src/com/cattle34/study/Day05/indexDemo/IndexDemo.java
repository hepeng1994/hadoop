package com.cattle34.study.Day05.indexDemo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IndexDemo {

    public static class IndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        /*原始文件是一个：hello rose*/
        String fileName = null;
        Text wordFile = new Text();
        IntWritable count = new IntWritable(1);
//获取文件名
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                /*输出：hello-a.html 1*/
                wordFile.set(word + "-" + fileName);
                context.write(wordFile, count);
            }

        }
    }

    public static class IndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /*输入：hello-a.html 1*/
            int allCount = 0;
            for (IntWritable value : values) {
                allCount += value.get();
            }
            context.write(key, new IntWritable(allCount));
        }
    }

    public static void main(String[] args) throws Exception {
        String path = "E:\\x\\Hadoop\\新hadoop\\day04\\mrdata\\index";
        String inputPath = path + "\\input";
        String outputPath = path + "\\output";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(IndexDemo.class);

        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*因为map端已经输出了结果，就不需要reduce了*/
//        job.setNumReduceTasks(0);


        FileSystem fs = FileSystem.get(conf);

        boolean exists = fs.exists(new Path(outputPath));

        if (exists) fs.delete(new Path(outputPath), true);


        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);


    }


}
