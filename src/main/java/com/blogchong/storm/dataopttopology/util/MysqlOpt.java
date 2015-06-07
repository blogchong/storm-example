package com.blogchong.storm.dataopttopology.util;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author blogchong
 * @version 2015年06月07日 上午13:31:25
 * @Blog www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @email blogchong@163.com
 * @QQ_G 191321336
 * @Weixin: blogchong
 * @Des Mysql操作的封装
 */

@SuppressWarnings("serial")
public class MysqlOpt implements Serializable {

    public Connection conn = null;
    PreparedStatement statement = null;

    // 连接数据库
    public boolean connSQL(String host_p, String database, String username,
                           String password) {

        String url = "jdbc:mysql://" + host_p + "/" + database
                + "?characterEncoding=UTF-8";

        try {

            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);
            return true;

        } catch (ClassNotFoundException cnfex) {

            System.out
                    .println("MysqlBolt-- Error: Loading JDBC/ODBC dirver failed!");
            cnfex.printStackTrace();

        } catch (SQLException sqlex) {

            System.out.println("MysqlBolt-- Error: Connect database failed!");
            sqlex.printStackTrace();

        }
        return false;
    }

    // 插入数据
    public boolean insertSQL(String sql) {
        try {

            statement = conn.prepareStatement(sql);
            statement.executeUpdate();
            return true;

        } catch (SQLException e) {

            System.out.println("MysqlBolt-- Error: Insert database failed!");
            e.printStackTrace();

        } catch (Exception e) {

            System.out.println("MysqlBolt-- Error: Insert failed!");
            e.printStackTrace();
        }

        return false;
    }

    // 关闭连接
    public void deconnSQL() {
        try {

            if (conn != null)
                conn.close();

        } catch (Exception e) {

            System.out.println("MysqlBolt-- Error: Deconnect database failed!");
            e.printStackTrace();
        }
    }

}
