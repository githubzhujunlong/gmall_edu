package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.TradeCourseOrderBean;
import com.atguigu.gmall_edu.bean.TradeProvinceOrderBean;
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

public class DWS_08_DWSProvinceOrder extends BaseApp {
    public static void main(String[] args) {
        new DWS_08_DWSProvinceOrder().init(
                3348,
                2,
                "DWS_08_DWSProvinceOrder",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeProvinceOrderBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<TradeProvinceOrderBean> aggStream = windowAndAgg(parseStream);
        //aggStream.print();

        SingleOutputStreamOperator<TradeProvinceOrderBean> dimStream = joinDims(aggStream);
        //dimStream.print();

        writeToClickhouse(dimStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<TradeProvinceOrderBean> dimStream) {
        dimStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_trade_province_order_window",
                TradeProvinceOrderBean.class
        ));
    }

    private SingleOutputStreamOperator<TradeProvinceOrderBean> joinDims(SingleOutputStreamOperator<TradeProvinceOrderBean> aggStream) {
        return AsyncDataStream.unorderedWait(
                aggStream,
                new AsyncDimFunction<TradeProvinceOrderBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_BASE_PROVINCE";
                    }

                    @Override
                    public String getId(TradeProvinceOrderBean input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public void joinDim(TradeProvinceOrderBean input, JSONObject dim) {
                        input.setProvinceName(dim.getString("NAME"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<TradeProvinceOrderBean> windowAndAgg(SingleOutputStreamOperator<TradeProvinceOrderBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(TradeProvinceOrderBean::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.getOrderSet().addAll(value2.getOrderSet());
                        value1.getUserSet().addAll(value2.getUserSet());
                        value1.setFinalAmount(value1.getFinalAmount().add(value2.getFinalAmount()));
                        return value1;
                    }
                }, new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        TradeProvinceOrderBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        bean.setOrderCount((long) bean.getOrderSet().size());
                        bean.setUserCount((long) bean.getUserSet().size());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TradeProvinceOrderBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(json -> json.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeProvinceOrderBean>() {

                    private ValueState<TradeProvinceOrderBean> lastBeanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastBeanState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<TradeProvinceOrderBean>(
                                        "lastBeanState",
                                        TradeProvinceOrderBean.class
                                ));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<TradeProvinceOrderBean> out) throws Exception {
                        long ts = value.getLong("ts") * 1000;
                        HashSet<String> orderSet = new HashSet<>();
                        orderSet.add(value.getString("order_id"));
                        HashSet<String> userSet = new HashSet<>();
                        userSet.add(value.getString("user_id"));
                        TradeProvinceOrderBean bean = TradeProvinceOrderBean.builder()
                                .orderDetailId(ctx.getCurrentKey())
                                .provinceId(value.getString("province_id"))
                                .finalAmount(value.getBigDecimal("final_amount"))
                                .orderSet(orderSet)
                                .userSet(userSet)
                                .ts(ts)
                                .build();

                        TradeProvinceOrderBean lastBean = lastBeanState.value();
                        if (lastBean != null){
                            lastBean.setFinalAmount(bean.getFinalAmount().subtract(lastBean.getFinalAmount()));
                            out.collect(lastBean);
                        }else {
                            out.collect(bean);
                        }

                        lastBeanState.update(bean);
                    }
                });
    }


}
