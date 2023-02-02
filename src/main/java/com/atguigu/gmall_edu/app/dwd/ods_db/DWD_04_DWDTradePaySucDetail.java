package com.atguigu.gmall_edu.app.dwd.ods_db;

import com.atguigu.gmall_edu.app.BaseSqlApp;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DWD_04_DWDTradePaySucDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD_04_DWDTradePaySucDetail().init(
                3337,
                2,
                "DWD_03_DWDTradePaySuc"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        readOdsDb(tableEnv, "DWD_03_DWDTradePaySuc");

        // 过滤支付表数据
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['order_id'] order_id, " +
                "data['create_time'] create_time, " +
                "data['payment_type'] payment_type, " +
                "ts " +
                "from ods_db " +
                "where `database` = 'gmall_edu' " +
                "and `table` = 'payment_info' " +
                "and type = 'insert' " +
                "or (" +
                "       type = 'update' " +
                "       and data['callback_time'] is not null " +
                "   )");
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // 读取kafka中下单事实表
        tableEnv.executeSql("create table order_detail(" +
                "id string, " +
                "course_id string, " +
                "course_name string, " +
                "order_id string, " +
                "user_id string, " +
                "province_id string, " +
                "final_amount string, " +
                "create_time string, " +
                "sc string " +
                ")" + SqlUtil.getKafkaSourceConnector(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, "DWD_03_DWDTradePaySuc"));

        Table result = tableEnv.sqlQuery("select " +
                "pi.id, " +
                "pi.order_id, " +
                "pi.create_time, " +
                "pi.payment_type, " +
                "od.course_id, " +
                "od.course_name, " +
                "od.province_id, " +
                "od.user_id, " +
                "od.final_amount order_detail_amount, " +
                "od.sc, " +
                "ts " +
                "from payment_info pi " +
                "join order_detail od " +
                "on pi.order_id = od.order_id");

        tableEnv.executeSql("create table dwd_trade_pay_suc_detail(" +
                "id string, " +
                "order_id string, " +
                "create_time string, " +
                "payment_type string, " +
                "course_id string, " +
                "course_name string, " +
                "province_id string, " +
                "user_id string, " +
                "order_detail_amount string, " +
                "sc string, " +
                "ts bigint" +
                ")" + SqlUtil.getKafkaSinkConnector(Constant.TOPIC_DWD_TRADE_PAY_SUC_DETAIL));
        result.executeInsert("dwd_trade_pay_suc_detail");
    }
}
