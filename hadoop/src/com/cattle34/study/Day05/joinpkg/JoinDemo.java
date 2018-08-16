package com.cattle34.study.Day05.joinpkg;

import com.cattle34.study.Day05.MRquchong.QuChong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.ArrayList;

public class JoinDemo {
    public static class JoinMap extends Mapper<LongWritable,Text,Text,JoinBeen>{

        String name =null;
        //注意FileSplit的导包  setup会先于map运行
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //获取了文件转换格式格式信息1
            FileSplit split = (FileSplit)context.getInputSplit();
            //获得文件名字
             name = split.getPath().getName();
        }
        //把订单与用户分别封装输出   ,用uid做key,把uid相同的分到一组
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            JoinBeen been = new JoinBeen();
            //u001,senge,18,male,angelababy
            //别把null直接写进去,要以字符串形式写进去,要不然空指针
            if ("user.txt".equals(name)){
                been.setOid("null");
               been.setUid(split[0]);
               been.setName(split[1]);
               been.setAge(Integer.parseInt(split[2]));
               been.setSex(split[3]);
               been.setFriend(split[4]);
            }else {
                been.setOid(split[0]);
                been.setUid(split[1]);
                been.setName("null");
                been.setAge(0);
                been.setSex("null");
                been.setFriend("null");
            }
            context.write(new Text(been.uid),been);
        }
    }
    public static  class JoinReduce extends Reducer<Text,JoinBeen,JoinBeen,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<JoinBeen> values, Context context) throws IOException, InterruptedException {
            JoinBeen user = new JoinBeen();
            ArrayList<JoinBeen> ord=new ArrayList<JoinBeen>();
            for (JoinBeen value : values) {
                //用oid是否为null区分出用户产生的对象和订单产生的对象
                if ("null".equals(value.oid)){
                    //代表用户been
                    user.setOid(value.oid);
                    user.setFriend(value.friend);
                    user.setName(value.name);
                    user.setUid(value.uid);
                    user.setAge(value.age);
                    user.setSex(value.sex);
                }else{
                    //代表订单been
                    JoinBeen ordbeen = new JoinBeen();
                   ordbeen.setOid(value.oid);
                   ordbeen.setFriend(value.friend);
                   ordbeen.setName(value.name);
                   ordbeen.setUid(value.uid);
                   ordbeen.setAge(value.age);
                   ordbeen.setSex(value.sex);
                    ord.add(ordbeen);
                }
            }
            //遍历订单集合输出,别把他放进上边for循环内`
            for (JoinBeen o : ord) {
                o.setFriend(user.friend);
                o.setName(user.name);
                o.setAge(user.age);
                o.setSex(user.sex);
                context.write(o,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path path1 = new Path("E:\\x\\Hadoop\\新hadoop\\day04\\mrdata\\join");
        Path in = new Path(path1 + "\\input");
        Path out = new Path(path1 + "\\output");
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);
        //要运行的程序
        job.setJarByClass(JoinDemo.class);
        //要运行的mapper与reduce
        job.setMapperClass(JoinMap.class);
        job.setReducerClass(JoinReduce.class);
        //map与reduce输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBeen.class);
        job.setOutputKeyClass(JoinBeen.class);
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
