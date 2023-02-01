package com.atguigu.gmall_edu.util;

import com.atguigu.gmall_edu.bean.TableProcess;
import com.atguigu.gmall_edu.common.Constant;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static Connection getPhoenixConnection(){
        return getJdbcConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL, null, null);
    }

    public static Connection getJdbcConnection(String driver, String url, String username, String password) {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeConnection(Connection connection){
        if (connection != null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static Connection getMysqlConnection() {
        return getJdbcConnection(
                Constant.MYSQL_DRIVER,
                Constant.MYSQL_URL,
                "root",
                "123456"
        );
    }

    public static <T> List<T> queryList(Connection conn, String sql, Object[] args, Class<T> tClass, boolean isUnderLineToCamel) throws Exception {
        ArrayList<T> result = new ArrayList<>();
        PreparedStatement ps = conn.prepareStatement(sql);
        if (args != null){
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
        }
        ResultSet resultSet = ps.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            T t = tClass.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String colName = metaData.getColumnLabel(i);
                if (isUnderLineToCamel){
                    colName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, colName);
                }
                Object value = resultSet.getObject(i);
                BeanUtils.setProperty(t, colName, value);
            }
            result.add(t);
        }
        return result;
    }
}
