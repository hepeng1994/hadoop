package com.cattle34.study.day02.mrpkg.handMapperReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.codehaus.jackson.util.BufferRecycler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 阶段二
 */
public class WordCountReduceDemo {
    public static void main(String[] args) throws Exception {
        /*1. 目标：统计阶段一的输出，在hdfs的 /wordcount/output/*/
        /*输入的文件路径*/

        String inputPath = args[0];
        /*作用，就是区分是模0还是模1*/
        int pNum = Integer.parseInt(args[1]);

        /*d定义一个存储结果的容器：map《word， 最终的词频》*/
        HashMap<String, Integer> wordCountMap = new HashMap<>();

        /*2. 开始解析：数据样式：python,1*/
        /**
         *
         * 2018-06-22 19:22 /wordcount/output/m-0-0
         * 2018-06-22 19:22 /wordcount/output/m-0-1
         * 2018-06-22 19:31 /wordcount/output/m-1-0
         * 2018-06-22 19:31 /wordcount/output/m-1-1
         */
        FileSystem fs = FileSystem.get(new URI("hdfs://xiaoniu1:9000/"), new Configuration(), "root");

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(inputPath), false);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            /*判断是0区还是1区*/
            if (next.getPath().getName().endsWith(pNum + "")) {
                /*开始读文件了*/
                FSDataInputStream in = fs.open(new Path(inputPath + next.getPath().getName()));

                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line = null;
                while ((line = br.readLine()) != null) {
                    /*解析：python,1*/
                    String[] wordCount = line.split(",");

                    int count = wordCountMap.getOrDefault(wordCount[0], 0);

                    wordCountMap.put(wordCount[0], count + 1);
                }

            }

        }


        /*输出文件*/
        FSDataOutputStream out = fs.create(new Path("/wordcount/r_output/" + "r-" + pNum));
        /*遍历map，写出结果*/
        Set<Map.Entry<String, Integer>> wordCountEntry = wordCountMap.entrySet();
        for (Map.Entry<String, Integer> finalWordCount : wordCountEntry) {
            /*word \t count*/
            out.write((finalWordCount.getKey() + "\t" + finalWordCount.getValue() + "\n").getBytes());

        }

        out.close();
        fs.close();


    }


}
