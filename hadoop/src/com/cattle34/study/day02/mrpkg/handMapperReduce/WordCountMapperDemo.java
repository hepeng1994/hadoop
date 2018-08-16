package com.cattle34.study.day02.mrpkg.handMapperReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

/*更改类名的快捷键：   shift + F6*/
public class WordCountMapperDemo {
    /**
     * 需求：统计在hdfs上 a.txt 文本中俄word的   词频
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        /*1. 从main方法中接受参数  ：目标文件地址*/

        String inPutPaht = args[0];
        /*接受的文件块的偏移量*/
        long offSet = Long.parseLong(args[1]);
        /*读取的文件块的长度*/
        long len = Long.parseLong(args[2]);
        /*读取的块的编号*/
        int taskNum = Integer.parseInt(args[3]);
//        new Configuration().set()

        FileSystem fs = FileSystem.get(new URI("hdfs://xiaoniu1:9000/"), new Configuration(), "root");
        /*2. 读取hdfs的文件 a.txt*/
        FSDataInputStream in = fs.open(new Path(inPutPaht));
        /*数据不同，逻辑相同*/
        in.seek(offSet);

        BufferedReader br = new BufferedReader(new InputStreamReader(in));/*按照行读取文件*/


        /*得判断一下是不是第一块*/
        if (offSet != 0) {
            br.readLine();/*此代码的含义就是：我多读一行，但是不参与计算，相当于跳过该行*/
        }


        /*判断hdfs上的输出目录是否存在*/
        if (!fs.exists(new Path("/wordcount/output/"))) fs.mkdirs(new Path("/wordcount/output/"));
        /*此out0代表往 阶段一的0块上写，0区的文件*/
        FSDataOutputStream out0 = fs.create(new Path("/wordcount/output/" + "m-" + taskNum + "-0"));
        FSDataOutputStream out1 = fs.create(new Path("/wordcount/output/" + "m-" + taskNum + "-1"));


        String line = null;
        long count = 0l;/*真实读取的文件长度*/
        while ((line = br.readLine()) != null) {
            /*此处得到的line是一行一行的文本数据：   java hello scala...*/
            String[] words = line.split(" ");
            for (String word : words) {
                /*在此处对我们的word进行分区操作了*/
                if (word.hashCode() % 2 == 0) {
                    /*往0区走,就是往hdfs上写文件 m-0-0*/
                    /*内容：   word， 1 这种*/
                    out0.write((word + ",1\n").getBytes());
                } else {
                    /*往1区走,就是往hdfs上写文件 m-0-1*/
                    out1.write((word + ",1\n").getBytes());
                }
            }
            /*对读取的真实的长度的统计累加，*/
            count += line.length() + 1;

            /*因为我们分了好几块，每一块都有  startoffset  和   该块的总长度*/
            /* 数据不同，逻辑相同*/
            /*真实的长度大于 块的长度，就相当于多都了一行*/
            if (count > len) break;

        }

        out0.close();
        out1.close();
        br.close();
        in.close();
        fs.close();

    }

}
