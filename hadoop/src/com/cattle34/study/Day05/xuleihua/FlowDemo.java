package com.cattle34.study.Day05.xuleihua;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class FlowDemo {

    public static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        Text phoneNum = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] flowFields = value.toString().split("\t");

            FlowBean flowBean = new FlowBean(
                    flowFields[1],
                    Long.parseLong(flowFields[flowFields.length - 3]),
                    Long.parseLong(flowFields[flowFields.length - 2])
//                    flowFields[flowFields.length-2]
            );
            phoneNum.set(flowBean.getPhoneNum());

            context.write(phoneNum, flowBean);


        }
    }


    public static class FlowReducer extends Reducer<Text, FlowBean, FlowBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            /*一个手机号的所有 flowbeans*/
            long sumUpFlow = 0l;
            long sumDownFlow = 0l;
            for (FlowBean flowBean : values) {
                sumUpFlow += flowBean.getUpFlow();
                sumDownFlow += flowBean.getDownFlow();
            }

            /*上面是计算上下行各自的总流量，还需要计算总流量*/
            FlowBean flowBean = new FlowBean(key.toString(), sumUpFlow, sumDownFlow);

            context.write(flowBean, NullWritable.get());

        }
    }

    public static void main(String[] args) throws Exception {
        String path = "D:\\resource\\datas\\mrdata\\flow\\";
        String inputPath = path + "input";
        String outputPath = path + "output1111";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(FlowDemo.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        /*指定序列化输出文件*/
        /*请注意：如果序列化的文件，有任何的改动，都需要重新进行序列化操作*/
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);


        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);

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

