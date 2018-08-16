package com.cattle34.study.day01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

public class HDFSDmeo2 {
    /*文件写入*/
    @Test
    public void testWrite() throws Exception {
        /*目标：是和hdfs进行通信*/
        /**
         * 参数1：hdfs集群的访问地址
         * 参数2：hdfs集群的配置文件
         * 参数3：访问hdfs的用户
         */
        FileSystem fs = FileSystem.get(new URI("hdfs://hdp01:9000/"), new Configuration(), "root");
        FSDataOutputStream out = fs.create(new Path("/34th.txt"));

        out.write("34th very good,guys very bang!!!\n".getBytes());

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        bw.write("haoqiang haoqiang!!!");
        bw.newLine();

        bw.close();
        out.close();
        fs.close();
    }


    /*读文件1*/
    @Test
    public void testRead() throws Exception {

        FileSystem fs = FileSystem.get(new URI("hdfs://hdp01:9000/"), new Configuration(), "root");

        FSDataInputStream in = fs.open(new Path("/34th.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line = null;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        fs.close();
    }


    /*读文件2：精确的读取数据，按照偏移量进行读取数据内容*/
    @Test
    public void testRead2() throws Exception {

        FileSystem fs = FileSystem.get(new URI("hdfs://hdp01:9000/"), new Configuration(), "root");

        FSDataInputStream in = fs.open(new Path("/34th.txt"));
        in.seek(10);/*从第10个字节开始读取*/
//        34th very good,guys very bang!!!
//        haoqiang haoqiang!!!
        byte[] b = new byte[3];/*每隔4个字节读一次*/

        /*进行读取*/
        int line = -1;
        int len = 0;/*记录读取的文件的总长度，如果等于9了，就停止*/
        while ((line = in.read(b)) != -1) {
            System.out.print(new String(b, 0, line));
            len += line;
//            good    ,guy     s ve
            if (len == 9) break;
        }

        in.close();
        fs.close();
    }
}
