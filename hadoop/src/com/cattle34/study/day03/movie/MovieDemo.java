package com.cattle34.study.day03.movie;

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
import org.codehaus.jackson.map.ObjectMapper;


import java.io.File;
import java.io.IOException;

public class MovieDemo {

    public static class MovieMapper extends Mapper<LongWritable, Text, MovieBeen, NullWritable> {
        //{"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"
        /*
        注意书写moviebeen时,要implements WritableComparable <MovieBeen>  重写write与reduce .compare
        @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.movie);
        out.writeInt(this.rate);
        out.writeLong(this.timeStamp);
        out.writeUTF(this.uid);
    }
    ************this一定要写上
    @Override
    public void readFields(DataInput in) throws IOException {
        this.movie = in.readUTF();
        this.rate= in.readInt();
        this.timeStamp = in.readLong();
        this.uid = in.readUTF();

 *******************************************************
 map 输出后 写一个类继承HashPartitioner<MovieBeen,NullWriter>   用作取膜分组
 *************************************************
然后在  框架默认对key惊醒快牌,
 ******************************************************

         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           try {
               String json = value.toString();
               ObjectMapper mapper = new ObjectMapper();
               MovieBeen movieBeen = mapper.readValue(json, MovieBeen.class);
               context.write(movieBeen, NullWritable.get());
           }catch (Exception e){


           }
        }
    }
   // 最后对每一对map输出的<key,value>做分组来判断是否是同一种电影,如果不判断,movie会随机输入,顺序换乱,然后进行输入到ruduce
    //MyGroupMovie就是实现了 WritableComparator ,内部重写了compare ,就是重写了分组条件,在进入reduce之前就是 默认调用movibeen中的compare
    public static class MovieReduce extends Reducer<MovieBeen, NullWritable, MovieBeen, NullWritable> {
        @Override
        protected void reduce(MovieBeen key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int top2 = 2;
            for (NullWritable value : values) {
                context.write(key, NullWritable.get());
                if (--top2 == 0) {
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration con = new Configuration();
        Job job = Job.getInstance(con);
        job.setJarByClass(MovieDemo.class);
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReduce.class);
        job.setOutputKeyClass(MovieBeen.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(MovieBeen.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setPartitionerClass(MyMoviePartition.class);
        job.setGroupingComparatorClass(MyGroupMovie.class);
        FileSystem fs = FileSystem.get(con);
        Path path = new Path("/movie/output");
        if (fs.exists(path)) fs.delete(path, true);
        FileInputFormat.setInputPaths(job, new Path("/movie/input"));
        FileOutputFormat.setOutputPath(job, new Path("/movie/output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);

    }
}
