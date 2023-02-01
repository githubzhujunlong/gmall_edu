package com.atguigu.gmall_edu.app.dwd.ods_db;

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
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

public class DWD_06_DWDBaseDbApp extends BaseApp {
    public static void main(String[] args) {
        new DWD_06_DWDBaseDbApp().init(
                3340,
                2,
                "DWD_06_DWDBaseDbApp",
                Constant.TOPIC_EDU_ODS_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        //etlStream.print();

        // 读取table_process
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcessFromMysql(env);
        //tpStream.print();

        // 合并流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStream = connectDwdAndTp(etlStream, tpStream);
        //connectStream.print();

        // 删除不需要的列
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> delColsStream = deleteNotNeedCols(connectStream);
        //delColsStream.print();

        // 写入kafka
        writeToKafka(delColsStream);
    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> delColsStream) {
        delColsStream.addSink(FlinkSinkUtil.getKafkaSinkForAutoTopic());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> deleteNotNeedCols(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStream) {
        return connectStream
                .map(t -> {
                    JSONObject data = t.f0;
                    TableProcess tp = t.f1;

                    List<String> cols = Arrays.asList(tp.getSinkColumns().split(","));
                    data.keySet().removeIf(key -> !cols.contains(key));
                    return t;
                })
                .returns(new TypeHint<Tuple2<JSONObject, TableProcess>>() {});
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectDwdAndTp(SingleOutputStreamOperator<JSONObject> etlStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        MapStateDescriptor<String, TableProcess> tpState = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcastStream = tpStream.broadcast(tpState);
        return etlStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {

                    private Map<String, TableProcess> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection conn = JdbcUtil.getMysqlConnection();
                        String sql = "select * from table_process where sink_type=?";
                        List<TableProcess> tpList = JdbcUtil.queryList(conn, sql, new Object[]{"dwd"}, TableProcess.class, true);
                        map = new HashMap<>();
                        for (TableProcess tp : tpList) {
                            String key = tp.getSourceTable() + ":" + tp.getSourceType();
                            map.put(key, tp);
                        }
                        System.out.println("预加载完成：" + map);
                        JdbcUtil.closeConnection(conn);
                    }

                    @Override
                    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpState);
                        String key = value.getString("table") + ":" + value.getString("type");
                        TableProcess tp = state.get(key);
                        if (tp == null){
                            System.out.println("状态中没有，正在尝试从集合中找。。。");
                            tp = map.get(key);
                            if (tp == null){
                                System.out.println("集合中也没有。。。");
                            }
                        }

                        if (tp != null){
                            JSONObject data = value.getJSONObject("data");
                            out.collect(Tuple2.of(data, tp));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcess value, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpState);
                        String key = value.getSourceTable() + ":" + value.getSourceType();
                        String op = value.getOp();
                        if("d".equals(op)){
                            state.remove(key);
                            map.remove(key);
                        }else {
                            state.put(key, value);
                        }
                    }
                });
    }

    private SingleOutputStreamOperator<TableProcess> readTableProcessFromMysql(StreamExecutionEnvironment env) {
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
                .map(line -> JSON.parseObject(line.replace("bootstrap-","")));
    }
}
