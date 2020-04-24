package com.xkj.mlrc.clean.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Properties;


/**
 * JDBC辅助组件
 *
 * @author Administrator
 */
public class JdbcHelper {

    static Logger loggrt = Logger.getLogger(JdbcHelper.class);
    private  static DruidDataSource druidDataSource = null;
    private  static JdbcHelper instance = null;



    /**
     * 获取单例
     *
     * @return 单例
     */
    public static JdbcHelper getHiveInstance() throws Exception {
        if (instance == null) {
            instance = new JdbcHelper();
            Properties properties = new Properties();
            String url = PropsUtil.getProp("hive.jdbc.url");
            String user = PropsUtil.getProp("hive.jdbc.user");
            String password = PropsUtil.getProp("hive.jdbc.password");
            String driver = PropsUtil.getProp("hive.jdbc.driver");
            properties.put("driverClassName",driver);
            properties.put("url",url);
            properties.put("username",user);
            properties.put("password",password);
            druidDataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
        }
        return instance;
    }


    /**
     * 返回druid数据库连接
     *
     * @return
     * @throws SQLException
     */
    public DruidPooledConnection getConnection() throws SQLException {

        return druidDataSource.getConnection();
    }

    /**
     * 执行增删改SQL语句
     *
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public int executeUpdate(String sql, Object[] params) {
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }

            rtn = pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            loggrt.error(e.toString(), e);
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    loggrt.error(e.toString(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    loggrt.error(e.toString(), e);
                }
            }
        }

        return rtn;
    }

    /**
     * 执行查询SQL语句
     *
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql, Object[] params,
                             QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }

            rs = pstmt.executeQuery();

            callback.process(rs);
        } catch (Exception e) {
            loggrt.error(e.toString(), e);
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    loggrt.error(e.toString(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    loggrt.error(e.toString(), e);
                }
            }
        }
    }

    /**
     * 执行查询SQL语句
     * @param sql

     */
    public void execute(String sql) {
        Connection conn = null;
        Statement pstmt = null;

        try {
            conn = getConnection();
            pstmt = conn.createStatement();
            pstmt.execute(sql);
        } catch (Exception e) {
            loggrt.error(e.toString(), e);
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    loggrt.error(e.toString(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    loggrt.error(e.toString(), e);
                }
            }
        }
    }

    /**
     * @param sql
     * @param paramsList
     * @return 每条SQL语句影响的行数
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();

            // 第一步：使用Connection对象，取消自动提交
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sql);

            // 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
            if (paramsList != null && !paramsList.isEmpty()) {
                for (Object[] params : paramsList) {
                    for (int i = 0; i < params.length; i++) {
                        pstmt.setObject(i + 1, params[i]);
                    }
                    pstmt.addBatch();
                }
            }

            // 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            rtn = pstmt.executeBatch();

            // 最后一步：使用Connection对象，提交批量的SQL语句
            conn.commit();
        } catch (Exception e) {
            loggrt.error(e.toString(), e);
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    loggrt.error(e.toString(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    loggrt.error(e.toString(), e);
                }
            }
        }

        return rtn;
    }

    /**
     * 静态内部类：查询回调接口
     *
     * @author Administrator
     */
    public static interface QueryCallback {

        /**
         * 处理查询结果
         *
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;

    }

}
