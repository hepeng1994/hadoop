package com.cattle34.study.day02.mrpkg.mapperAndReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import org.apache.hadoop.mapreduce.lib.FileInputFormat;


public class WCDemo_Linux_yarn {

    public static void main(String[] args) throws Exception {
        /*模拟提交mr程序*/
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf);

//        job.setJar();
        job.setJarByClass(WCDemo_Linux_yarn.class);
        /*告知平台，我们要运行的是哪个具体的业务的实现map类*/
        job.setMapperClass(WordCountMapper1.class);
        /*告知平台，我们要运行的是哪个具体的业务的实现reduce类*/
        job.setReducerClass(WordCountReduce1.class);

        /*告知自己map的输出key和value*/
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        /*最终程序的运行结果的key和value*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*输入文件在哪里*/
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        /*输出文件在哪里*/
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output333/"));

        /*火箭的助推器，卫星提交完成之后，程序就终止了*/
//        job.submit();
        /*卫星和地面控制中心， 一直关注卫星的运行情况，看mr程序的运行状况*/
        boolean b = job.waitForCompletion(true);
        /*当b返回true，就返回 0， 反之就返回 非0的数据*/
        System.exit(b ? 0 : -1);
    }

}
