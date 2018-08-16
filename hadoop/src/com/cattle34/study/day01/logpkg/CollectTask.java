package com.cattle34.study.day01.logpkg;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;
import java.util.UUID;

public class CollectTask extends TimerTask {
    @Override
    public void run() {
        try {
            Properties prop = PropertiesUtils_Lazy.getProp();
//        System.out.println("开始收集");
            /*1. 找见日志数据的存放目录*/
            File sourceDir = new File(prop.getProperty(Commparams.SOURCEDIR));
            /*2. 过滤出有效的日志数据:
             * 为什么过滤：因为两个文件：一个正在写入的：access.log     access.log.1
             * 另一个生成好的：access.log.*    */
            File[] tarGetFils = sourceDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.startsWith(prop.getProperty(Commparams.PREFIX));
                }
            });

            /*3. 将滤出有效的日志数据   转移到待上传的目录下：  toUpDir*/

            File toUpDir = new File(prop.getProperty(Commparams.TOUPDIR));
            if (!toUpDir.exists()) toUpDir.mkdirs();

            for (File tarGetFil : tarGetFils) {
                tarGetFil.renameTo(new File(toUpDir.getPath()+"/"+ tarGetFil.getName()));
            }

            /*4. 开始上传hdfs上*/

            FileSystem fs = FileSystem.get(new URI("hdfs://xiaoniu1:9000/"), new Configuration(), "root");

            /*5. 想/apps/logs/2018-06-20/存不存在*/
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            /*判断备份目录存不存在*/
            File backDir = new File(prop.getProperty(Commparams.BACKDIR) +"/"+ sdf.format(date));
            if (!backDir.exists()) backDir.mkdirs();


            Path hdfsPath = new Path("/apps/logs/" + sdf.format(date));

            if (!fs.exists(hdfsPath)) fs.mkdirs(hdfsPath);

            /*6. 正式上传*/
            File[] toUpLogs = toUpDir.listFiles();
            for (File toUpLog : toUpLogs) {
                /*本地的路径，     hdfs的路径*/
                fs.copyFromLocalFile(new Path(toUpLog.getPath()), new Path(hdfsPath.toString() + "/" + toUpLog.getName() + "-" + UUID.randomUUID()));
                /*7. 传完之后，需要将已经传了的日志文件，移动到备份目录下：backDir*/
                toUpLog.renameTo(new File(backDir.getPath() + "/" + toUpLog.getName()));
            }


            fs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
