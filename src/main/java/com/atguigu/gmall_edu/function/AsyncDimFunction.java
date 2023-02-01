package com.atguigu.gmall_edu.function;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> {

    private ThreadPoolExecutor pool;

    public abstract String getTable();

    public abstract String getId(T input);

    public abstract void joinDim(T input, JSONObject dim);

    @Override
    public void open(Configuration parameters) throws Exception {
        pool = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        pool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Jedis jedis = RedisUtil.getRedisClient();
                    DruidPooledConnection conn = DruidDSUtil.getPhoenixConn();

                    JSONObject dim = DimUtil.readDimFromRedisAndPhoenix(jedis, conn, getTable(), getId(input));
                    joinDim(input, dim);
                    resultFuture.complete(Collections.singletonList(input));

                    JdbcUtil.closeConnection(conn);
                    jedis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
