package com.cattle34.study.Day05.masqlMR;

import java.sql.*;
import java.util.HashMap;

public class UserHandler {
    static Connection conn = null;
    static PreparedStatement preparedStatement = null;
    static ResultSet rs = null;

    public static void init(HashMap<String, UserBean> userMap) {
//        获取mysql中的所有user信息
        /*jdbc的连接，取msyql中的用户数据*/

        try {
            String url = "jdbc:mysql://xiaoniu/mydb?"
                    + "user=root&password=123456&useUnicode=true&characterEncoding=UTF8";


            // 之所以要使用下面这条语句，是因为要使用MySQL的驱动，所以我们要把它驱动起来，
            // 可以通过Class.forName把它加载进去，也可以通过初始化来驱动起来，下面三种形式都可以
            Class.forName("com.mysql.jdbc.Driver");// 动态加载mysql驱动

            conn = DriverManager.getConnection(url);

            preparedStatement = conn.prepareStatement("select * from t_user");

            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                String uid = rs.getString(1);
                String name = rs.getString(2);
                String age = rs.getString(3);
                String gender = rs.getString(4);
                /*最终的结果给下一步*/
//                对userBean对象进行装配
                UserBean userBean = new UserBean(uid, name, age, gender);

                /*装配map*/
                userMap.put(uid, userBean);

            }

        } catch (Exception e) {

        }finally {
            /*扫尾：关闭所有的连接*/
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }
}
