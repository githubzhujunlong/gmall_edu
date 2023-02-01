package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.UserRegisterBean;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.util.FlinkSinkUtil;
import com.atguigu.gmall_edu.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DWS_05_DWSUserRegister extends BaseApp {
    public static void main(String[] args) {
        new DWS_05_DWSUserRegister().init(
                3344,
                2,
                "DWS_04_DWSUserRegister",
                Constant.TOPIC_DWD_USER_REGISTER
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<UserRegisterBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<UserRegisterBean> aggStream = windowAndAgg(parseStream);
        //aggStream.print();

        writeToClickhouse(aggStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<UserRegisterBean> aggStream) {
        aggStream.addSink(FlinkSinkUtil.getClickhouseSink("dws_user_user_register_window", UserRegisterBean.class));
    }

    private SingleOutputStreamOperator<UserRegisterBean> windowAndAgg(SingleOutputStreamOperator<UserRegisterBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setUserRegisterCount(value1.getUserRegisterCount() + value2.getUserRegisterCount());
                        return value1;
                    }
                }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        UserRegisterBean bean = values.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<UserRegisterBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(line -> {
                    JSONObject jsonObject = JSON.parseObject(line);
                    String create_time = jsonObject.getString("create_time");
                    long ts = TimeUtil.dateTimeToTs(create_time);

                    UserRegisterBean bean = UserRegisterBean.builder()
                            .userRegisterCount(1L)
                            .ts(ts)
                            .build();
                    return bean;
                });

    }
}
