package com.cattle34.study.Day05.masqlMR;

public class UserBean {

    private String uid;
    private String name;
    private String age;
    private String gender;

    public UserBean(String uid, String name, String age, String gender) {
        this.uid = uid;
        this.name = name;
        this.age = age;
        this.gender = gender;
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

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

}
