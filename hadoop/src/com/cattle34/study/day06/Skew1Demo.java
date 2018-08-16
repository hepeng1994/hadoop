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
import java.util.Random;

public class Skew1Demo {
    /*
    统计文件中字母的数量:最终格式 a 100000  b2
    数据倾斜就是在进入reduce是可能会出现分组严重失衡的状况比如a有1000万个,b有2ge,采取措施吧a分到b里面输入进行运算
    将结果输出到文件中,key 为a-0   value为 55万    a-1  45万    b2 这就是skew1的操作,
    在通过skew2进行读取skew1输出的文件 , map中以"-" 切分  做成  key为a  value55万 ,进行输出,
    redduce 接收的时候 以 a、b为 key ,我们只需要把 分其中两部分a 进行累加就行了

    虽然 代码多了 但是减少了 每个部分使用的资源 ,不会出现在一个地方堆积大量数据,造成运行缓慢的情况

     */


    /*输出 a-0 1*/
    public static class Skew1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Random random = null;
        int numReduceTasks = 1;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numReduceTasks = context.getNumReduceTasks();
            random = new Random();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(" ");
            for (String word : split) {
//              输出的key：  a-0    numReduceTasks为reduce个数   正好用作标记
                context.write(new Text(word + "-" + random.nextInt(numReduceTasks)), new IntWritable(1));
            }

        }
    }


    public static class Skew1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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
        String inputPath = path + "input";
        String outputPath = path + "output";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(Skew1Demo.class);

        job.setMapperClass(Skew1Mapper.class);
        job.setReducerClass(Skew1Reducer.class);


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
