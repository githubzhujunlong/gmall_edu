package com.atguigu.gmall_edu.app.dwd.ods_db;

import com.atguigu.gmall_edu.app.BaseSqlApp;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DWD_03_DWDTradeOrderDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD_03_DWDTradeOrderDetail().init(
                3336,
                2,
                "DWD_02_DWDTradeOrderDetail"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        readOdsDb(tableEnv, "DWD_02_DWDTradeOrderDetail");

        // 过滤order_detail表的数据
        Table orderDetail = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['course_id'] course_id, " +
                "data['course_name'] course_name, " +
                "data['order_id'] order_id, " +
                "data['user_id'] user_id, " +
                "data['final_amount'] final_amount, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from ods_db " +
                "where `database` = 'gmall_edu' " +
                "and `table` = 'order_detail' " +
                "and type = 'insert' ");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        // 读取页面日志
        readOdsLog(tableEnv, "DWD_02_DWDTradeOrderDetail");
        // 过滤出订单页面
        Table orderSource = tableEnv.sqlQuery("select " +
                "item, " +
                "sc " +
                "from page_log " +
                "where item_type = 'order_id' " +
                "and page_id = 'order' ");
        tableEnv.createTemporaryView("order_source", orderSource);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
        Table result = tableEnv.sqlQuery("select " +
                "id, " +
                "course_id, " +
                "course_name, " +
                "order_id, " +
                "user_id, " +
                "final_amount, " +
                "create_time, " +
                "sc, " +
                "ts " +
                "from order_detail od " +
                "left join order_source os " +
                "on od.order_id = os.item ");


        tableEnv.executeSql("create table dwd_trade_order_detail(" +
                "id string, " +
                "course_id string, " +
                "course_name string, " +
                "order_id string, " +
                "user_id string, " +
                "final_amount string, " +
                "create_time string, " +
                "sc string, " +
                "ts bigint, " +
                "primary key(id) not enforced" +
                ")" + SqlUtil.getUpsertKafkaSinkConnector(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        result.executeInsert("dwd_trade_order_detail");
    }
}
