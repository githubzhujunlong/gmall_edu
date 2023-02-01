package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.UserLoginBean;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.util.FlinkSinkUtil;
import com.atguigu.gmall_edu.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DWS_04_DWSUserLogin extends BaseApp {
    public static void main(String[] args) {
        new DWS_04_DWSUserLogin().init(
                3343,
                2,
                "DWS_03_DWSUserLogin",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<UserLoginBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<UserLoginBean> AggStream = windowAndAgg(parseStream);
        //AggStream.print();

        writeToClickhouse(AggStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<UserLoginBean> aggStream) {
        aggStream.addSink(FlinkSinkUtil.getClickhouseSink("dws_user_user_login_window", UserLoginBean.class));
    }

    private SingleOutputStreamOperator<UserLoginBean> windowAndAgg(SingleOutputStreamOperator<UserLoginBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setUserCount(value1.getUserCount() + value2.getUserCount());
                        value1.setBackCount(value1.getBackCount() + value2.getBackCount());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        UserLoginBean bean = values.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<UserLoginBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON:: parseObject)
                .filter(json -> json.getString("uid") != null)
                .keyBy(json -> json.getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastLoginDateState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<String>("lastLoginDateState", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<UserLoginBean> out) throws Exception {
                        Long ts = value.getLong("ts");
                        String today = TimeUtil.tsToDate(ts);
                        String lastLoginDate = lastLoginDateState.value();

                        Long userCount = 0L;
                        Long backCount = 0L;
                        if (!today.equals(lastLoginDate)){
                            userCount = 1L;
                            lastLoginDateState.update(today);
                            // 本次登录与上次登陆日期大于7天则视为回流用户
                            if (lastLoginDate != null && (ts - TimeUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24 > 7){
                                backCount = 1L;
                            }
                        }

                        UserLoginBean bean = UserLoginBean.builder()
                                .userCount(userCount)
                                .backCount(backCount)
                                .ts(ts)
                                .build();

                        out.collect(bean);
                    }
                });
    }
}
