package com.cattle34.study.Day05.masqlMR;

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
import java.sql.*;
import java.util.Arrays;

public class MysqlJoinDemo {

    /**
     * 关联用户信息和订单信息，
     * 用户信息：在mysql中
     * 订单信息：txt文件中
     */


    public static class MysqlJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            /*jdbc的连接，取msyql中的用户数据*/

            try {
                String url = "jdbc:mysql://xiaoniu1:3306/mydb?"
                        + "user=root&password=123456&useUnicode=true&characterEncoding=UTF8";


                // 之所以要使用下面这条语句，是因为要使用MySQL的驱动，所以我们要把它驱动起来，
                // 可以通过Class.forName把它加载进去，也可以通过初始化来驱动起来，下面三种形式都可以
                Class.forName("com.mysql.jdbc.Driver");// 动态加载mysql驱动

                conn = DriverManager.getConnection(url);

            } catch (Exception e) {

            }


        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*u01,order001,apple,5,8.5,   +  用户的信息就可以了*/
            String[] fields = value.toString().split(",");

            try {
                preparedStatement = conn.prepareStatement("select * from t_user where uid=?");
                /*按照用户id进行mysql中的查询*/
                preparedStatement.setString(1, fields[0]);
                rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    String uid = rs.getString(1);
                    String name = rs.getString(2);
                    String age = rs.getString(3);
                    String gender = rs.getString(4);
                    /*最终的结果给下一步*/
                    context.write(
                            new Text(Arrays.toString(fields) + "," + name + "," + age + "," + gender), NullWritable.get()
                    );
                }


            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            /*扫尾：关闭所有的连接*/
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String path = "E:\\x\\Hadoop\\新hadoop\\day04\\mrdata\\orderdata";
        String inputPath = path + "//input";
        String outputPath = path + "//output";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(MysqlJoinDemo.class);

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
