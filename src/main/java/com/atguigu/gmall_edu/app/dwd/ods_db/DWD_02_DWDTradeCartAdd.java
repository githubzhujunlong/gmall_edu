package com.atguigu.gmall_edu.app.dwd.ods_db;

import com.atguigu.gmall_edu.app.BaseSqlApp;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DWD_02_DWDTradeCartAdd extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD_02_DWDTradeCartAdd().init(
                3335,
                2,
                "DWD_01_DWDTradeCartAdd"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        readOdsDb(tableEnv,"DWD_01_DWDTradeCartAdd");

        // 过滤购物车表
        Table cartInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['course_id'] course_id, " +
                "data['course_name'] course_name," +
                "ts " +
                "from ods_db " +
                "where `database` = 'gmall_edu' " +
                "and `table` = 'cart_info' " +
                "and type = 'insert' ");

        // 写入kafka
        tableEnv.executeSql("create table dwd_trade_cart_add(" +
                "id string, " +
                "user_id string, " +
                "course_id string, " +
                "course_name string, " +
                "ts bigint " +
                ")" + SqlUtil.getKafkaSinkConnector(Constant.TOPIC_DWD_TRADE_CART_ADD));
        cartInfo.executeInsert("dwd_trade_cart_add");
    }
}
