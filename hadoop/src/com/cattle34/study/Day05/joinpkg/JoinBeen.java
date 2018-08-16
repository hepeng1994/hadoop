package com.cattle34.study.Day05.joinpkg;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinBeen implements Writable {
   public String oid;
   public String uid;
   public String name;
   public int age;
   public String sex;
   public String friend;

    public JoinBeen() {
    }

    public JoinBeen(String oid, String uid, String name, int age, String sex, String friend) {
        this.oid = oid;
        this.uid = uid;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.friend = friend;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getFriend() {
        return friend;
    }

    public void setFriend(String friend) {
        this.friend = friend;
    }

    @Override
    public String toString() {
        return "JoinBeen{" +
                "oid='" + oid + '\'' +
                ", uid='" + uid + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", sex='" + sex + '\'' +
                ", friend='" + friend + '\'' +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.oid);
        out.writeUTF(this.uid);
        out.writeUTF(this.name);
        out.writeInt(this.age);
        out.writeUTF(this.sex);
        out.writeUTF(this.friend);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.oid=in.readUTF();
        this.uid=in.readUTF();
        this.name=in.readUTF();
        this.age=in.readInt();
        this.sex=in.readUTF();
        this.friend=in.readUTF();
    }
}
