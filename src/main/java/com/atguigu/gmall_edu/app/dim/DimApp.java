package com.atguigu.gmall_edu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.TableProcess;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.util.FlinkSinkUtil;
import com.atguigu.gmall_edu.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().init(
                3333,
                2,
                "DimApp",
                Constant.TOPIC_EDU_ODS_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        //etlStream.print();

        // 读取维度配置信息
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        //tpStream.print();

        // 根据维度配置信息在phoenix中建表
        createTableForPhoenix(tpStream);

        // 合并流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dimAndTpStream = connectDimAndTp(etlStream, tpStream);
        //dimAndTpStream.print();

        // 删除不需要的列
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> delColsStream = deleteNotNeedCols(dimAndTpStream);
        //delColsStream.print();

        // 写入phoenix
        writeToPhoenix(delColsStream);
    }

    private void writeToPhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> delColsStream) {
        delColsStream.addSink(FlinkSinkUtil.getPhoenixSink());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> deleteNotNeedCols(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dimAndTpStream) {
        return dimAndTpStream
                .map(t -> {
                    JSONObject data = t.f0;
                    TableProcess tp = t.f1;

                    List<String> cols = Arrays.asList(tp.getSinkColumns().split(","));
                    data.keySet().removeIf(key -> !cols.contains(key));
                    return Tuple2.of(data, tp);
                })
                .returns(new TypeHint<Tuple2<JSONObject, TableProcess>>() {});
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectDimAndTp(SingleOutputStreamOperator<JSONObject> etlStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        MapStateDescriptor<String, TableProcess> tpState = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBroadCastStream = tpStream.broadcast(tpState);
        return etlStream.connect(tpBroadCastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {

                    private Map<String, TableProcess> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection conn = JdbcUtil.getMysqlConnection();
                        String sql = "select * from table_process where sink_type=?";
                        Object[] args = {"dim"};
                        // 给定sql语句，参数获取查询MySQL的结果
                        List<TableProcess> tpList = JdbcUtil.queryList(conn, sql, args, TableProcess.class, true);

                        // 预加载到map中
                        map = new HashMap<>();
                        for (TableProcess tp : tpList) {
                            String key = tp.getSourceTable();
                            map.put(key, tp);
                        }
                        System.out.println("预加载完成:" + map);
                        JdbcUtil.closeConnection(conn);
                    }

                    @Override
                    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpState);
                        String key = value.getString("table");
                        TableProcess tp = state.get(key);
                        if (tp == null){
                            System.out.println("状态中没有，正在尝试从集合中找");
                            tp = map.get(key);
                            if (tp == null){
                                System.out.println("集合中也没有");
                            }
                        }

                        if (tp != null){
                            out.collect(Tuple2.of(value.getJSONObject("data"), tp));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcess value, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpState);
                        String op = value.getOp();
                        String key = value.getSourceTable();
                        if ("d".equals(op)){
                            state.remove(key);
                            map.remove(key);
                        }else {
                            state.put(key, value);
                        }
                    }
                });
    }

    private void createTableForPhoenix(SingleOutputStreamOperator<TableProcess> tpStream) {
        tpStream
                .filter(tp -> "dim".equals(tp.getSinkType()))
                .process(new ProcessFunction<TableProcess, TableProcess>() {

                    private Connection conn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = JdbcUtil.getPhoenixConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        JdbcUtil.closeConnection(conn);
                    }

                    @Override
                    public void processElement(TableProcess value, Context ctx, Collector<TableProcess> out) throws Exception {
                        String op = value.getOp();
                        if ("d".equals(op)){
                            deleteTable(value);
                        }else if ("r".equals(op) || "c".equals(op)){
                            createTable(value);
                        }else {
                            deleteTable(value);
                            createTable(value);
                        }
                    }

                    private void createTable(TableProcess value) throws SQLException {
                        StringBuilder sql = new StringBuilder();
                        sql
                                .append("create table if not exists ")
                                .append(value.getSinkTable())
                                .append("(")
                                .append(value.getSinkColumns().replaceAll("[^,]+","$0 varchar"))
                                .append(", constraint pk primary key(")
                                .append(value.getSinkPk())
                                .append("))")
                                .append(value.getSinkExtend() == null ? "" : value.getSinkExtend());
                        System.out.println("phoenix建表语句：" + sql);

                        PreparedStatement ps = conn.prepareStatement(sql.toString());
                        ps.execute();
                        ps.close();
                    }

                    private void deleteTable(TableProcess value) throws SQLException {
                        String sql = "drop table if exists " + value.getSinkTable();
                        PreparedStatement ps = conn.prepareStatement(sql);
                        ps.execute();
                        ps.close();
                    }
                });
    }

    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("useSSL","false");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .databaseList("gmall_edu_config")
                .tableList("gmall_edu_config.table_process")
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .jdbcProperties(properties)
                .build();

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc")
                .map(line -> {
                    JSONObject jsonObject = JSON.parseObject(line);

                    String op = jsonObject.getString("op");
                    TableProcess tableProcess;
                    if ("d".equals(op)){
                        tableProcess = jsonObject.getObject("before", TableProcess.class);
                    }else {
                        tableProcess = jsonObject.getObject("after", TableProcess.class);
                    }
                    tableProcess.setOp(op);
                    return tableProcess;
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(line -> {
                    try {
                        JSONObject jsonObject = JSON.parseObject(line);
                        String type = jsonObject.getString("type");
                        String data = jsonObject.getString("data");
                        return "gmall_edu".equals(jsonObject.getString("database"))
                                && jsonObject.getString("table") != null
                                && ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2;
                    } catch (Exception e) {
                        System.out.println("数据不符合json格式：" + line);
                        return false;
                    }

                })
                .map(line -> JSON.parseObject(line.replace("bootstrap-", "")));
    }
}
