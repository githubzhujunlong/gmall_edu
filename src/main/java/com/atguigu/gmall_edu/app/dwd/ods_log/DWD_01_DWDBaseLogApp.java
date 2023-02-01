package com.atguigu.gmall_edu.app.dwd.ods_log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.util.FlinkSinkUtil;
import com.atguigu.gmall_edu.util.TimeUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

public class DWD_01_DWDBaseLogApp extends BaseApp {

    private static final String START = "start";
    private static final String ACTION = "action";
    private static final String DISPLAY = "display";
    private static final String PAGE = "page";
    private static final String ERR = "err";
    private static final String APPVIDEO = "appVideo";

    private HashMap<String, DataStream<JSONObject>> mapStream;

    public static void main(String[] args) {
        new DWD_01_DWDBaseLogApp().init(
                3334,
                2,
                "DWD_01_DWDBaseLogApp",
                Constant.TOPIC_EDU_ODS_LOG
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 读取日志数据并解析成jsonObject，由于flume中已经添加了拦截器，这里就不用在判断是否符合json格式了
        SingleOutputStreamOperator<JSONObject> parseStream = parseToJsonObject(stream);
        //parseStream.print();

        // 纠正新老用户状态
        SingleOutputStreamOperator<JSONObject> validateStream = validateNewOrOld(parseStream);
        //validateStream.print();

        // 分流
        HashMap<String, DataStream<JSONObject>> SplitStreamMap = splitStream(validateStream);
        //SplitStreamMap.get("page").print();

        // 写入kafka
        writeToKafka(SplitStreamMap);
    }

    private void writeToKafka(HashMap<String, DataStream<JSONObject>> splitStreamMap) {
        splitStreamMap.get(START).map(JSON::toString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        splitStreamMap.get(ACTION).map(JSON::toString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        splitStreamMap.get(DISPLAY).map(JSON::toString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        splitStreamMap.get(PAGE).map(JSON::toString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        splitStreamMap.get(ERR).map(JSON::toString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        splitStreamMap.get(APPVIDEO).map(JSON::toString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_APPVIDEO));
    }

    private HashMap<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validateStream) {
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page") {
        };
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("err") {
        };
        OutputTag<JSONObject> appVideoTag = new OutputTag<JSONObject>("appVideo") {
        };
        SingleOutputStreamOperator<JSONObject> startStream = validateStream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject common = value.getJSONObject("common");
                        Long ts = value.getLong("ts");

                        JSONObject start = value.getJSONObject("start");
                        if (start != null) {
                            start.putAll(common);
                            start.put("ts", ts);
                            out.collect(start);
                        }

                        JSONArray actions = value.getJSONArray("actions");
                        if (actions != null) {
                            for (int i = 0; i < actions.size(); i++) {
                                JSONObject action = actions.getJSONObject(i);
                                action.putAll(common);
                                action.put("ts", ts);
                                ctx.output(actionTag, action);
                            }
                        }

                        JSONArray displays = value.getJSONArray("displays");
                        if (displays != null) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                display.putAll(common);
                                display.put("ts", ts);
                                ctx.output(displayTag, display);
                            }
                        }

                        JSONObject page = value.getJSONObject("page");
                        if (page != null) {
                            page.putAll(common);
                            page.put("ts", ts);
                            ctx.output(pageTag, page);
                        }

                        JSONObject err = value.getJSONObject("err");
                        if (err != null) {
                            err.putAll(common);
                            err.put("ts", ts);
                            ctx.output(errTag, err);
                        }

                        JSONObject appVideo = value.getJSONObject("appVideo");
                        if (appVideo != null) {
                            appVideo.putAll(common);
                            appVideo.put("ts", ts);
                            ctx.output(appVideoTag, appVideo);
                        }
                    }
                });

        DataStream<JSONObject> actionStream = startStream.getSideOutput(actionTag);
        DataStream<JSONObject> displayStream = startStream.getSideOutput(displayTag);
        DataStream<JSONObject> pageStream = startStream.getSideOutput(pageTag);
        DataStream<JSONObject> errStream = startStream.getSideOutput(errTag);
        DataStream<JSONObject> appVideoStream = startStream.getSideOutput(appVideoTag);

        mapStream = new HashMap<>();

        mapStream.put("start", startStream);
        mapStream.put("action", actionStream);
        mapStream.put("display", displayStream);
        mapStream.put("page", pageStream);
        mapStream.put("err", errStream);
        mapStream.put("appVideo", appVideoStream);
        return mapStream;
    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> parseStream) {
        return parseStream
                .keyBy(json -> json.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> firstDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstDateState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<String>(
                                        "firstDate",
                                        String.class
                                ));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        JSONObject common = value.getJSONObject("common");
                        String is_new = common.getString("is_new");
                        Long ts = value.getLong("ts");
                        String today = TimeUtil.tsToDate(ts);
                        String firstDate = firstDateState.value();

                        // 如果是新用户
                        if ("1".equals(is_new)) {
                            if (firstDate == null) {
                                // 如果状态中没有数据，更新状态
                                firstDateState.update(today);
                            } else {
                                // 如果这次登陆的日期与状态中的日期不一致，纠正为老用户
                                if (!today.equals(firstDate)) {
                                    common.put("is_new", "0");
                                }
                            }
                        } else {
                            if (firstDate == null) {
                                String yesterday = TimeUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                firstDateState.update(yesterday);
                            }
                        }
                        return value;
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> parseToJsonObject(DataStreamSource<String> stream) {
        return stream.map(JSON::parseObject);
    }
}
