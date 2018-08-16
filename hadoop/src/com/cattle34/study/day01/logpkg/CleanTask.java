package com.cattle34.study.day01.logpkg;

import javax.xml.crypto.Data;
import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

public class CleanTask extends TimerTask {

    @Override
    public void run() {
        File file = new File("F:\\beifen");
        //获取备份目录
        File[] listFiles = file.listFiles();
        //时间转换
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");;
        //获取当天时间
        Date date = new Date();
        //注释为前一天时间
//        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date date=new Date();
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTime(date);
//        calendar.add(Calendar.DAY_OF_MONTH, -1);
//        date = calendar.getTime();
//        System.out.println(sdf.format(date));

        for (File listFile : listFiles) {
            //大于一天的删除
            try {
                if (date.getTime()-format.parse(listFile.getName()).getTime()>24*60*60*1000){
                    listFile.delete();
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }
}
