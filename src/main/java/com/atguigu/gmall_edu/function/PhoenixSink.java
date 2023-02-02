package com.atguigu.gmall_edu.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.bean.TableProcess;
import com.atguigu.gmall_edu.util.DimUtil;
import com.atguigu.gmall_edu.util.JdbcUtil;
import com.atguigu.gmall_edu.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private Connection conn;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = JdbcUtil.getPhoenixConnection();
        jedis = RedisUtil.getRedisClient();
    }

    @Override
    public void close() throws Exception {
        JdbcUtil.closeConnection(conn);
        jedis.close();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;


        writeToPhoenix(data, tp);

        delRedis(data, tp);
    }

    private void delRedis(JSONObject data, TableProcess tp) {
        String key = DimUtil.getKey(tp.getSinkTable(), data.getString("id")).toUpperCase();
        jedis.del(key);
    }

    private void writeToPhoenix(JSONObject data, TableProcess tp) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql
                .append("upsert into ")
                .append(tp.getSinkTable())
                .append("(")
                .append(tp.getSinkColumns())
                .append(") values(")
                .append(tp.getSinkColumns().replaceAll("[^,]+","?"))
                .append(")");
        System.out.println("phoenix插入语句：" + sql);

        // 填充占位符
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        String[] cols = tp.getSinkColumns().split(",");
        for (int i = 0; i < cols.length; i++) {
            ps.setString(i + 1, data.getString(cols[i]));
        }
        ps.execute();
        conn.commit();
        ps.close();
    }
}
