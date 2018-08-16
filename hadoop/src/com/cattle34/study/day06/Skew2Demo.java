package com.cattle34.study.day06;

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

import java.io.IOException;

public class Skew2Demo {

    public static class Skew2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        输入数据是：    a-1	891
//            目标输出：   a    891

            String[] wordCount = value.toString().split("-");
            context.write(new Text(wordCount[0]), new IntWritable(Integer.parseInt(wordCount[1].split("\t")[1])));
        }
    }


    public static class Skew2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int allCount = 0;
            for (IntWritable value : values) {
                allCount+=value.get();
            }
            context.write(key, new IntWritable(allCount));
        }
    }


    public static void main(String[] args) throws Exception {
        String path = "D:\\resource\\datas\\mrdata\\skew\\";
        String inputPath = path + "output";
        String outputPath = path + "output2222";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(Skew2Demo.class);

        job.setMapperClass(Skew2Mapper.class);
        job.setReducerClass(Skew2Reducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*因为map端已经输出了结果，就不需要reduce了*/
        job.setNumReduceTasks(2);


        FileSystem fs = FileSystem.get(conf);

        boolean exists = fs.exists(new Path(outputPath));

        if (exists) fs.delete(new Path(outputPath), true);


        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);


    }
}
