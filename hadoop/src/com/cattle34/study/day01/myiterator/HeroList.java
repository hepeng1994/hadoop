package com.cattle34.study.day01.myiterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class HeroList implements Iterable <Hero>{
    @Override
    public Iterator iterator() {
        return new MyIterator();
    }
}
class Test {
    public static void main(String[] args) {
        //创建list
        ArrayList<Hero> heroes = new ArrayList<Hero>();
        HeroList list = new HeroList();
        for (Hero hero : list) {
            Hero hero1 = new Hero();
            hero1.setKillNum(hero.getKillNum());
            hero1.setName(hero.getName());
            hero1.setKillNum(hero.getKillNum());
            heroes.add(hero1);
        }
        //排序
        Collections.sort(heroes, new Comparator<Hero>() {
            @Override
            public int compare(Hero o1, Hero o2) {
                return o2.killNum-o1.killNum;
            }
        });
        for (Hero hero : heroes) {
            System.out.println(hero);
        }

    }
}
