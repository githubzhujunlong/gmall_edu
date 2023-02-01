package com.atguigu.gmall_edu.app.dws;

import com.atguigu.gmall_edu.app.BaseSqlApp;
import com.atguigu.gmall_edu.bean.TrafficKeyWordBean;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.function.KwSplit;
import com.atguigu.gmall_edu.util.FlinkSinkUtil;
import com.atguigu.gmall_edu.util.SqlUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DWS_02_DWSTrafficKeyWord extends BaseSqlApp {
    public static void main(String[] args) {
        new DWS_02_DWSTrafficKeyWord().init(
                3342,
                2,
                "DWS_02_DWSTrafficKeyWord"
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // 读取页面日志
        tableEnv.executeSql("create table page_log(" +
                "item string, " +
                "item_type string, " +
                "ts bigint, " +
                "et as to_timestamp_ltz(ts,3), " +
                "watermark for et as et - interval '3' second" +
                ")" + SqlUtil.getKafkaSourceConnector(Constant.TOPIC_DWD_TRAFFIC_PAGE, "DWS_02_DWSTrafficKeyWord"));

        // 过滤搜索操作
        Table itemTable = tableEnv.sqlQuery("select " +
                "item, " +
                "et " +
                "from page_log " +
                "where item_type = 'keyword' " +
                "and item is not null ");
        tableEnv.createTemporaryView("item_table", itemTable);

        // 开窗聚合
        Table result = tableEnv.sqlQuery("select " +
                "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                "item keyword, " +
                "count(1) keyword_count, " +
                "unix_timestamp() * 1000 as ts " +
                "from table(tumble(table item_table, descriptor(et), interval '5' seconds)) " +
                "group by window_start,window_end,item ");

        // 转换为撤回流
        SingleOutputStreamOperator<TrafficKeyWordBean> keywordStream = tableEnv.toRetractStream(result, TrafficKeyWordBean.class)
                .filter(t -> t.f0)
                .map(t -> t.f1);

        // 写入clickhouse
        keywordStream.addSink(FlinkSinkUtil.getClickhouseSink("dws_traffic_keyword_window", TrafficKeyWordBean.class));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
