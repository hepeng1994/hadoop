package com.cattle34.study.day01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.testng.annotations.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;

public class HDFSDemo {
    @Test
    public void testmkdir() throws Exception {
        //创建文件
        FileSystem fs = FileSystem.get(new URI("hdfs://xiaoniu1:9000/"), new Configuration(), "root");
        fs.mkdirs(new Path("/ddd/"));
        fs.close();
    }

    @Test
    public void testUp() throws Exception {
        //上传
        FileSystem fs = FileSystem.get(new URI("hdfs://xiaoniu1:9000/"), new Configuration(), "root");
        fs.copyFromLocalFile(new Path("E:\\x\\JAVA基础\\day02\\day02\\视频/"), new Path("/"));
        fs.close();
    }

    @Test
    public void testDel() throws Exception {
        //删除
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://xiaoniu1:9000/");
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("/apps/logs/2018-06-21/"), true);
        fs.close();
    }

    @Test
    public void testRemove() throws Exception {
        //  重命名
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://xiaoniu1:9000/");
        FileSystem fs = FileSystem.get(conf);
        fs.rename(new Path("/ddd/"), new Path("/123/"));
        fs.close();
    }

    @Test
    public void testGet() throws Exception {
        //  下载
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://xiaoniu1:9000/");
        FileSystem fs = FileSystem.get(conf);
        fs.copyToLocalFile(new Path("/apps/"), new Path("F:/"));
        fs.close();
    }

    @Test
    public void testLs() throws Exception {
        //  查询
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://xiaoniu1:9000/");
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus next = files.next();
            System.out.println("getAccessTime:" + next.getAccessTime());
            System.out.println("getBlockSize:" + next.getBlockSize());
            System.out.println("getModificationTime:" + next.getModificationTime());
            System.out.println("getLen:" + next.getLen());
            System.out.println("getPath:" + next.getPath());
            System.out.println("getReplication:" + next.getReplication());
            System.out.println("getGroup:" + next.getGroup());
            System.out.println("getPermission:" + next.getPermission());
            System.out.println("getOwner:" + next.getOwner());
            System.out.println("---------------------------------------------------");
            //有三个块
            BlockLocation[] locations = next.getBlockLocations();
            for (BlockLocation location : locations) {
                System.out.println("getLength:" + location.getLength());
                System.out.println("getOffset:" + location.getOffset());
                System.out.println("getCachedHosts:" + Arrays.toString(location.getCachedHosts()));
                System.out.println("getHosts:" + Arrays.toString(location.getHosts()));
                System.out.println("getNames:" + Arrays.toString(location.getNames()));
                System.out.println("getStorageTypes:" + Arrays.toString(location.getStorageTypes()));
                System.out.println("getStorageIds:" + Arrays.toString(location.getStorageIds()));
                System.out.println("getTopologyPaths:" + Arrays.toString(location.getTopologyPaths()));
                System.out.println("*********************************************************************");
            }
            fs.close();

        }
    }
        @Test
        public void testLS2() throws Exception {
            //  查询
            System.setProperty("HADOOP_USER_NAME", "root");
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://xiaoniu1:9000/");
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(new Path("/"));
            for (FileStatus fileStatus : status) {
                System.out.println("getPath"+fileStatus.getPath());
            }
            fs.close();
        }
    }



