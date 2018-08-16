package com.cattle34.study.anli;


//import org.apache.commons.lang3.StringUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class LogProcessDemo {

    public static class LogProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        ObjectMapper objectMapper = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            objectMapper = new ObjectMapper();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*1. 解析json日志数据*/
            String jsonLine = value.toString();
            /*2. b)并去掉"events"字段, 只处理header的内容*/
            JsonNode jsonNode = objectMapper.readTree(jsonLine);
            JsonNode header = jsonNode.get("header");
            /*过滤掉一些不合法数据（缺失device_id，app_ver_name,os_name，app_token，city,release_channel 字 段需要过滤）*/
            if (StringUtils.isBlank(header.get("device_id").getTextValue())
                    || StringUtils.isBlank(header.get("app_ver_name").getTextValue())
                    || StringUtils.isBlank(header.get("os_name").getTextValue())
                    || StringUtils.isBlank(header.get("app_token").getTextValue())
                    || StringUtils.isBlank(header.get("city").getTextValue())
                    || StringUtils.isBlank(header.get("release_channel").getTextValue())
                    ) {
                return;
            } else {
                /*正式的解析文本文件，获取字段，解析成每一行的文本*/
                StringBuilder sb = new StringBuilder();

                /*c)追加一个字段  user_id（如果是苹果，就用  device_id,如果是android，就用  android_id）*/
                String user_id = "";
                if (header.get("os_name").getTextValue().equals("ios")) {
                    /*是苹果系统*/
                    user_id = header.get("device_id").getTextValue();
                } else {
                    /*是安卓系统*/
                    user_id = header.get("android_id").getTextValue();
                }
                sb.append(header.get("cid_sn").getTextValue()).append("\001");
                sb.append(header.get("mobile_data_type").getTextValue()).append("\001");
                sb.append(header.get("os_ver").getTextValue()).append("\001");
                sb.append(header.get("mac").getTextValue()).append("\001");
                sb.append(header.get("resolution").getTextValue()).append("\001");
                sb.append(header.get("commit_time").getTextValue()).append("\001");
                sb.append(header.get("sdk_ver").getTextValue()).append("\001");
                sb.append(header.get("device_id_type").getTextValue()).append("\001");
                sb.append(header.get("city").getTextValue()).append("\001");
                sb.append(header.get("android_id").getTextValue()).append("\001");
                sb.append(header.get("device_model").getTextValue()).append("\001");
                sb.append(header.get("carrier").getTextValue()).append("\001");
                sb.append(header.get("promotion_channel").getTextValue()).append("\001");
                sb.append(header.get("app_ver_name").getTextValue()).append("\001");
                sb.append(header.get("imei").getTextValue()).append("\001");
                sb.append(header.get("app_ver_code").getTextValue()).append("\001");
                sb.append(header.get("pid").getTextValue()).append("\001");
                sb.append(header.get("net_type").getTextValue()).append("\001");
                sb.append(header.get("device_id").getTextValue()).append("\001");
                sb.append(header.get("app_device_id").getTextValue()).append("\001");
                sb.append(header.get("release_channel").getTextValue()).append("\001");
                sb.append(header.get("country").getTextValue()).append("\001");
                sb.append(header.get("time_zone").getTextValue()).append("\001");
                sb.append(header.get("os_name").getTextValue()).append("\001");
                sb.append(header.get("manufacture").getTextValue()).append("\001");
                sb.append(header.get("commit_id").getTextValue()).append("\001");
                sb.append(header.get("app_token").getTextValue()).append("\001");
                sb.append(header.get("account").getTextValue()).append("\001");
                sb.append(header.get("app_id").getTextValue()).append("\001");
                sb.append(header.get("build_num").getTextValue()).append("\001");
                sb.append(header.get("language").getTextValue()).append("\001");
                sb.append(user_id);

                context.write(new Text(sb.toString()), NullWritable.get());

            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogProcessDemo.class);

        job.setMapperClass(LogProcessMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        FileInputFormat.setInputPaths(job, new Path("E:\\hadoop-34\\day04\\mrdata\\applog\\input\\20170101"));
//        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop-34\\day04\\mrdata\\applog\\output"));


        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);

    }

}
