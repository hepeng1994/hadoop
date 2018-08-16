package com.cattle34.study.day01.logpkg;

import java.util.Timer;

public class CollectDemo {
    public static void main(String[] args) {
        //s定期清理或者收集日志
        Timer time = new Timer();
        //此项目分为两部分 ,第一部分:手机日志并上传过
       time.schedule(new CollectTask(),0,5*1000);
        //第二部分:清空待上传的目录,然后在日志服务器里备份最近一天的视频
        time.schedule(new CleanTask(),0,5*1000);
    }
}
