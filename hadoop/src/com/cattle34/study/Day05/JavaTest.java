package com.cattle34.study.Day05;

/**
 * Created by wei.ma on 2016/11/15.
 */
public class JavaTest {
    public static void main(String[] args) {

//        三条简洁说明：
//
//        1 class.getResource("/")是从classpath开始寻找资源文件
//
//        2 class.getResource("")是从当前包开始寻找资源文件
//
//        3 classLoader.getResource("")是从classpath开始寻找资源文件

//        使用ClassLoader的getResource方法的时候，永远是以Classpath为直接起点开始寻找资源的。不用担心从什么包开始寻找的问题。
//         Class.getResourceAsStream(String path) ： path 不以’/'开头时默认是从此类所在的包下取资源，以’/'开头则是从ClassPath根下获取。其只是通过path构造一个绝对路径，最终还是由ClassLoader获取资源。

//         Class.getClassLoader.getResourceAsStream(String path) ：默认则是从ClassPath根下获取，path不能以’/'开头，最终是由ClassLoader获取资源。
//        路径前不带'/'，则是相对路径；若带，则是绝对路径。（这个绝对只是在工程内的绝对，并不绝对于磁盘）
//        总结一下，就是你想获得文件，你得从最终生成的.class文件为着手点，不要以.java文件的路径为出发点，因为真正使用的就是.class，不会拿个.java文件就使用，因为java是编译型语言嘛
        System.out.println("path不以’/'开头时，默认是从此类所在的包下取资源：" + JavaTest.class.getResource(""));
        System.out.println("path  以’/'开头时，则是从ClassPath根下获取；path以’/'开头时，则是从ClassPath根下获取；】在这里就是相当于bin目录" +
                "" + JavaTest.class.getResource("/"));


        // 当前类(class)所在的包目录
        System.out.println(JavaTest.class.getResource(""));
        // class path根目录
        System.out.println(JavaTest.class.getResource("/"));

        // JavaTest.class在<bin>/testpackage包中
        // 2.properties  在<bin>/testpackage包中
        System.out.println(JavaTest.class.getResource("subpkg/3.properties"));

        // JavaTest.class在<bin>/testpackage包中
        // 3.properties  在<bin>/testpackage.subpackage包中
        System.out.println(JavaTest.class.getResource("lineAnLi")+"llalalal");

        // JavaTest.class在<bin>/testpackage包中
        // 1.properties  在bin目录（class根目录）
        System.out.println(JavaTest.class.getResource("/1.properties"));


        JavaTest t = new JavaTest();
        System.out.println(t.getClass());
        System.out.println(t.getClass().getClassLoader());
        System.out.println("path是从ClassPath根下获取" + t.getClass().getClassLoader().getResource(""));
        System.out.println("path不能以’/'开头.." + t.getClass().getClassLoader().getResource("/"));//null
//        JavaTest.class.getResource("/") == t.getClass().getClassLoader().getResource("")

        System.out.println(t.getClass().getClassLoader().getResource(""));

        System.out.println(t.getClass().getClassLoader().getResource("1.properties"));
        System.out.println(t.getClass().getClassLoader().getResource("com/test/2.properties"));
        System.out.println(t.getClass().getClassLoader().getResource("com/test/subpkg/3.properties"));
    }
}
