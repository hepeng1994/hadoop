package com.cattle34.study.Day05.xuleihua;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDemo2 {

    public static class Flow2Mapper extends Mapper<FlowBean, NullWritable, FlowBean, NullWritable>{
        @Override
        protected void map(FlowBean key, NullWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
    public static class Flow2Reduce extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {
        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());

        }
    }


    public static void main(String[] args) throws Exception {
        String path = "D:\\resource\\datas\\mrdata\\flow\\";
        String inputPath = path + "output1111";
        String outputPath = path + "output2222";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(FlowDemo2.class);

        job.setMapperClass(Flow2Mapper.class);
        job.setReducerClass(Flow2Reduce.class);
        /*指定序列化输入文件*/
        job.setInputFormatClass(SequenceFileInputFormat.class);
        /*告知job，重写了partition方法*/
        job.setPartitionerClass(MySortPartition.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);


        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);

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
