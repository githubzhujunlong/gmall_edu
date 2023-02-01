package com.atguigu.gmall_edu.util;

import com.atguigu.gmall_edu.common.Constant;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

public class SqlUtil {
    public static String getKafkaSourceConnector(String topic, String groupId, String... formats){
        String format = "json";
        if (formats.length > 0){
            format = formats[0];
        }
        return "WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                "  'properties.group.id' = '" + groupId + "', " +
                "  'scan.startup.mode' = 'latest-offset', " +
                "  'format' = '" + format + "' " +
                ")";
    }

    public static String getKafkaSinkConnector(String topic, String... formats){
        String format = "json";
        if (formats.length > 0){
            format = formats[0];
        }
        return "WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                "  'format' = '" + format + "' " +
                ")";
    }

    public static String getUpsertKafkaSinkConnector(String topic, String... formats){
        String format = "json";
        if (formats.length > 0){
            format = formats[0];
        }
        return " WITH (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'key.format' = '" + format + "', " +
                "  'value.format' = '" + format + "' " +
                ")";
    }

    public static Set<String> ikSplit(String item){
        Set<String> set = new HashSet<>();

        StringReader reader = new StringReader(item);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        try {
            Lexeme next = ikSegmenter.next();
            while (next != null){
                String keyword = next.getLexemeText();
                set.add(keyword);
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return set;
    }
}
