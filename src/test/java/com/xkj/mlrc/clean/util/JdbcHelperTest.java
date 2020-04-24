package com.xkj.mlrc.clean.util;


import org.junit.Test;

import java.sql.*;


/**
 * @author lijf@2345.com
 * @date 2020/4/23 15:55
 * @desc
 */

public class JdbcHelperTest {

    @Test
    public void getConnection() throws Exception {
        JdbcHelper jdbcHelper = JdbcHelper.getHiveInstance();
        Connection connection = jdbcHelper.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from all_hdfs_path");
        while (resultSet.next()){
            System.out.println(resultSet.getString(1));

        }
    }
}
