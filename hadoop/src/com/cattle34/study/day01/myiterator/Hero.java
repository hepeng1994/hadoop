package com.cattle34.study.day01.myiterator;

public class Hero {
    public  String num;
    public  String name;
    public  int killNum;

    public Hero(String num, String name, int killNum) {
        this.num = num;
        this.name = name;
        this.killNum = killNum;
    }

    public Hero() {

    }

    public String getNum() {
        return num;
    }

    public void setNum(String num) {
        this.num = num;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getKillNum() {
        return killNum;
    }

    public void setKillNum(int killNum) {
        this.killNum = killNum;
    }

    @Override
    public String toString() {
        return "Hero{" +
                "num='" + num + '\'' +
                ", name='" + name + '\'' +
                ", killNum=" + killNum +
                '}';
    }
}
