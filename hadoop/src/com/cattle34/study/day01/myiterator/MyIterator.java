package com.cattle34.study.day01.myiterator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class MyIterator implements Iterator<Hero> {
    // 读取数据
    private BufferedReader br = null;
    private String line = null;

    public MyIterator() {
        inte();
    }

    public void inte() {
        try {
            br = new BufferedReader(new FileReader("D:\\ideaIU\\project\\hadoop\\src\\com\\cattle34\\study\\day01\\myiterator\\hero.txt"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        //判断是否有下一行
        try {
            line = br.readLine();
            return line != null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Hero next() {
        String[] split = line.split("#");
        Hero hero = new Hero();
        hero.setNum(split[0]);
        hero.setName(split[1]);
        hero.setKillNum(Integer.parseInt(split[2]));
        return hero;
    }

    public static void main(String[] args) {
        //创建list
        ArrayList<Hero> arrayList = new ArrayList<>();
        MyIterator iterator = new MyIterator();
        while (iterator.hasNext()) {
            Hero next = iterator.next();
            arrayList.add(next);
        }
        //排序
        Collections.sort(arrayList, new Comparator<Hero>() {
            @Override
            public int compare(Hero o1, Hero o2) {
                return o2.killNum - o1.killNum;
            }
        });
        //查看输出
        for (Hero hero : arrayList) {
            System.out.println(hero);

        }
    }
}
