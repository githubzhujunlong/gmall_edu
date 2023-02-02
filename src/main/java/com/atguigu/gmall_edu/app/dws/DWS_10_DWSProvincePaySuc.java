package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.TradePaySucBean;
import com.atguigu.gmall_edu.bean.TradeSourceOrderBean;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.function.AsyncDimFunction;
import com.atguigu.gmall_edu.util.FlinkSinkUtil;
import com.atguigu.gmall_edu.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DWS_10_DWSProvincePaySuc extends BaseApp {
    public static void main(String[] args) {
        new DWS_10_DWSProvincePaySuc().init(
                3350,
                2,
                "DWS_09_DWSProvincePaySuc",
                Constant.TOPIC_DWD_TRADE_PAY_SUC_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradePaySucBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<TradePaySucBean> aggStream = windowAndAgg(parseStream);
        //aggStream.print();

        SingleOutputStreamOperator<TradePaySucBean> dimStream = joinDims(aggStream);
        //dimStream.print();

        writeToClickhouse(dimStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<TradePaySucBean> dimStream) {
        dimStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_trade_pay_suc_window",
                TradePaySucBean.class
        ));
    }

    private SingleOutputStreamOperator<TradePaySucBean> joinDims(SingleOutputStreamOperator<TradePaySucBean> aggStream) {
        return AsyncDataStream.unorderedWait(
                aggStream,
                new AsyncDimFunction<TradePaySucBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_BASE_PROVINCE";
                    }

                    @Override
                    public String getId(TradePaySucBean input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public void joinDim(TradePaySucBean input, JSONObject dim) {
                        input.setProvinceName(dim.getString("NAME"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<TradePaySucBean> windowAndAgg(SingleOutputStreamOperator<TradePaySucBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradePaySucBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(TradePaySucBean::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradePaySucBean>() {
                    @Override
                    public TradePaySucBean reduce(TradePaySucBean value1, TradePaySucBean value2) throws Exception {
                        value1.getUserSet().addAll(value2.getUserSet());
                        return value1;
                    }
                }, new WindowFunction<TradePaySucBean, TradePaySucBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradePaySucBean> input, Collector<TradePaySucBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        TradePaySucBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        bean.setUserCount((long) bean.getUserSet().size());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TradePaySucBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(line -> {
                    JSONObject json = JSON.parseObject(line);
                    HashSet<String> userSet = new HashSet<>();
                    userSet.add(json.getString("user_id"));
                    TradePaySucBean bean = TradePaySucBean.builder()
                            .provinceId(json.getString("province_id"))
                            .userSet(userSet)
                            .ts(json.getLong("ts") * 1000)
                            .build();
                    return bean;
                });


    }


}
