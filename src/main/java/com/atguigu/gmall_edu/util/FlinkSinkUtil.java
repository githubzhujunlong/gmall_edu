package com.atguigu.gmall_edu.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.bean.TableProcess;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.function.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlinkSinkUtil {
    public static PhoenixSink getPhoenixSink() {
        return new PhoenixSink();
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        properties.put("transaction.timeout.ms", 15 * 60 * 1000);

        return new FlinkKafkaProducer<String>(
                "kafka",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(topic, element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>> getKafkaSinkForAutoTopic(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        properties.put("transaction.timeout.ms", 15 * 60 * 1000);

        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
                "kafka",
                new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> element, @Nullable Long timestamp) {
                        JSONObject data = element.f0;
                        TableProcess tp = element.f1;
                        return new ProducerRecord<byte[], byte[]>(tp.getSinkTable(), data.toJSONString().getBytes(StandardCharsets.UTF_8));
                    }
                },
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static <T> SinkFunction<T> getClickhouseSink(String table, Class<T> tClass, boolean... isCamelToUnderLine){
        StringBuilder sql = new StringBuilder();

        // 获取传入bean的属性
        Field[] fields = tClass.getDeclaredFields();
        boolean flag = true;
        if (isCamelToUnderLine.length > 0){
            flag = isCamelToUnderLine[0];
        }
        boolean finalFlag = flag;
        String cols = Stream.of(fields)
                .map(f -> {
                    if (finalFlag){
                        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, f.getName());
                    }
                    return f.getName();
                })
                .collect(Collectors.joining(","));
        sql
                .append("insert into ")
                .append(table)
                .append("(")
                .append(cols)
                .append(") values(")
                .append(cols.replaceAll("[^,]+","?"))
                .append(")");
        System.out.println("clickhouse建表语句：" + sql);

        // 填充占位符
        String driver = Constant.CLICKHOUSE_DRIVER;
        String url = Constant.CLICKHOUSE_URL;

        return JdbcSink.sink(
                sql.toString(),
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        Field[] fields = tClass.getDeclaredFields();
                        try {
                            for (int i = 0; i < fields.length; i++) {
                                Field field = fields[i];
                                field.setAccessible(true);
                                Object value = field.get(t);
                                preparedStatement.setObject(i + 1, value);
                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1024 * 1024)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(driver)
                        .withUrl(url)
                        .build()
        );

    }
}
