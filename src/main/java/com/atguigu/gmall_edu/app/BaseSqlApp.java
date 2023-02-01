package com.atguigu.gmall_edu.app;

import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.util.FlinkSourceUtil;
import com.atguigu.gmall_edu.util.SqlUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSqlApp {

    public void init(int port, int parallelism, String groupId){
        // webui
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        // 开启checkpoint
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/user/flink/gmall_edu/checkpoint" + groupId);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 子类需要实现的方法
        handle(env,tableEnv);

    }

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv);

    public static void readOdsDb(StreamTableEnvironment tableEnv, String groupId){
        // 读取全部业务数据
        tableEnv.executeSql("create table ods_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>, " +
                "ts bigint, " +
                "pt as proctime() " +
                ")" + SqlUtil.getKafkaSourceConnector(Constant.TOPIC_EDU_ODS_DB, groupId));

    }

    public static void readOdsLog(StreamTableEnvironment tableEnv, String groupId){
        // 读取页面日志数据
        tableEnv.executeSql("create table page_log(" +
                "item string, " +
                "item_type string, " +
                "last_page_id string, " +
                "page_id string, " +
                "uid string, " +
                "sc string " +
                ")" + SqlUtil.getKafkaSourceConnector(Constant.TOPIC_DWD_TRAFFIC_PAGE, groupId));

    }
}
