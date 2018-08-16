package com.cattle34.study.Day05.masqlMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class MysqlJoinDemo2 {

    /**
     * 关联用户信息和订单信息，
     * 用户信息：在mysql中
     * 订单信息：txt文件中
     */


    public static class MysqlJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        HashMap<String, UserBean> userMap = new HashMap<String, UserBean>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            /*对用户数据的初始化*/
            UserHandler.init(userMap);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*u01,order001,apple,5,8.5,   +  用户的信息就可以了*/
            String[] fields = value.toString().split(",");

            UserBean userBean = userMap.get(fields[0]);
            /*最终的结果给下一步*/
            context.write(
                    new Text(Arrays.toString(fields) + "," + userBean.getName() + "," + userBean.getAge() + "," + userBean.getGender()), NullWritable.get()
            );

        }

    }


    public static void main(String[] args) throws Exception {
        String path = "D:\\resource\\datas\\mrdata\\orderdata\\";
        String inputPath = path + "input";
        String outputPath = path + "output";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(MysqlJoinDemo2.class);

        job.setMapperClass(MysqlJoinMapper.class);


        job.setMapOutputKeyClass(Text.class);   /*订单id*/

        /*因为map端已经输出了结果，就不需要reduce了*/
        job.setNumReduceTasks(0);


        FileSystem fs = FileSystem.get(conf);

        boolean exists = fs.exists(new Path(outputPath));

        if (exists) fs.delete(new Path(outputPath), true);


        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);


    }

}
