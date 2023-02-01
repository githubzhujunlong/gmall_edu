package com.atguigu.gmall_edu.app;

import com.atguigu.gmall_edu.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseApp {

    public void init(int port, int parallelism, String groupId, String topic){
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

        // 读取kafka
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(groupId, topic);

        DataStreamSource<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        // 子类需要实现的方法
        handle(env,stream);

        try {
            // 设置job的名字
            env.execute(groupId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);
}