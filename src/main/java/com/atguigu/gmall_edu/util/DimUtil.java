package com.atguigu.gmall_edu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.common.Constant;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {
    public static JSONObject readDimFromRedisAndPhoenix(Jedis jedis, Connection connection, String table, String id) throws Exception {
        JSONObject dim = readFromRedis(jedis, table, id);
        if (dim == null){
            System.out.println(getKey(table, id) + " 查询的数据库");
            dim = readDimFromPhoenix(connection, table, id);
            writeToRedis(jedis, table, id, dim);
        }else {
            System.out.println(getKey(table, id) + " 查询的redis");
        }
        return dim;
    }

    private static void writeToRedis(Jedis jedis, String table, String id, JSONObject dim) {
        String key = getKey(table, id);
        jedis.setex(key, Constant.TWO_DAYS_SECOND, dim.toJSONString());
    }

    public static String getKey(String table, String id){
        return table + ":" + id;
    }

    public static JSONObject readFromRedis(Jedis jedis, String table, String id){
        String json = jedis.get(getKey(table, id));
        if (json != null){
            return JSON.parseObject(json);
        }
        return null;
    }

    public static JSONObject readDimFromPhoenix(Connection connection, String table, String id) throws Exception {
        StringBuilder sql = new StringBuilder();
        sql
                .append("select * from ")
                .append(table)
                .append(" where id=?");
        List<JSONObject> list = JdbcUtil.queryList(
                connection,
                sql.toString(),
                new Object[]{id},
                JSONObject.class,
                false
        );

        if (list.size() > 0){
            return list.get(0);
        }else {
            return new JSONObject();
        }

    }

    public static void main(String[] args) throws Exception {
        JSONObject json = readDimFromPhoenix(
                JdbcUtil.getPhoenixConnection(),
                "dim_base_source",
                "1"
        );
        System.out.println(json);
    }
}
