package com.cattle34.study.Day05.indexDemo;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Index2Demo {

    /**
     * 整体目标：hello
     * 	c.html --> 6    a.html --> 4    b.html -->4
     */
    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*输入数据是：hello-a.html   4*/
            /*输出：   hello   c.html --> 6    a.html --> 4    b.html -->4 */
            String[] wordHtmlCount = value.toString().split("-");
            //TODO
            context.write(new Text(wordHtmlCount[0]), new Text(wordHtmlCount[1].replace("\t", "-->")));
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value).append("\t");
            }
            /*输出结果*/
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        String path = "E:\\x\\Hadoop\\新hadoop\\day04\\mrdata\\index";
        String inputPath = path + "\\output";
        String outputPath = path + "\\output1111";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(Index2Demo.class);

        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
