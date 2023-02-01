package com.atguigu.gmall_edu.common;

public class Constant {
    public static final String KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String TOPIC_EDU_ODS_LOG = "edu_ods_log";
    public static final String TOPIC_EDU_ODS_DB = "edu_ods_db";

    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306/gmall_edu_config?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_APPVIDEO = "dwd_traffic_appVideo";

    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_PAY_SUC_DETAIL = "dwd_trade_pay_suc_detail";
    public static final String TOPIC_DWD_INTERACTION_COMMENT = "dwd_interaction_comment";
    public static final String TOPIC_DWD_EXAM_EXAM_QUESTION = "dwd_exam_exam_question";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final String CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/gmall_edu";
    public static final int TWO_DAYS_SECOND = 24 * 60 * 60;
}
