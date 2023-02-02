package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.TradeProvinceOrderBean;
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

public class DWS_09_DWSSourceOrder extends BaseApp {
    public static void main(String[] args) {
        new DWS_09_DWSSourceOrder().init(
                3349,
                2,
                "DWS_09_DWSSourceOrder",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeSourceOrderBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<TradeSourceOrderBean> aggStream = windowAndAgg(parseStream);
        //aggStream.print();

        SingleOutputStreamOperator<TradeSourceOrderBean> dimStream = joinDims(aggStream);
        //dimStream.print();

        writeToClickhouse(dimStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<TradeSourceOrderBean> dimStream) {
        dimStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_trade_source_order_window",
                TradeSourceOrderBean.class
        ));
    }

    private SingleOutputStreamOperator<TradeSourceOrderBean> joinDims(SingleOutputStreamOperator<TradeSourceOrderBean> aggStream) {
        return AsyncDataStream.unorderedWait(
                aggStream,
                new AsyncDimFunction<TradeSourceOrderBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_BASE_SOURCE";
                    }

                    @Override
                    public String getId(TradeSourceOrderBean input) {
                        return input.getSourceId();
                    }

                    @Override
                    public void joinDim(TradeSourceOrderBean input, JSONObject dim) {
                        input.setSourceName(dim.getString("SOURCE_SITE"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<TradeSourceOrderBean> windowAndAgg(SingleOutputStreamOperator<TradeSourceOrderBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeSourceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(TradeSourceOrderBean::getSourceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeSourceOrderBean>() {
                    @Override
                    public TradeSourceOrderBean reduce(TradeSourceOrderBean value1, TradeSourceOrderBean value2) throws Exception {
                        value1.getOrderSet().addAll(value2.getOrderSet());
                        value1.getUserSet().addAll(value2.getUserSet());
                        value1.setFinalAmount(value1.getFinalAmount().add(value2.getFinalAmount()));
                        return value1;
                    }
                }, new WindowFunction<TradeSourceOrderBean, TradeSourceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSourceOrderBean> input, Collector<TradeSourceOrderBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        TradeSourceOrderBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        bean.setOrderCount((long) bean.getOrderSet().size());
                        bean.setUserCount((long) bean.getUserSet().size());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TradeSourceOrderBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .filter(json -> json.getString("sc") != null)
                .keyBy(json -> json.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeSourceOrderBean>() {

                    private ValueState<TradeSourceOrderBean> lastBeanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastBeanState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<TradeSourceOrderBean>(
                                        "lastBeanState",
                                        TradeSourceOrderBean.class
                                ));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<TradeSourceOrderBean> out) throws Exception {
                        long ts = value.getLong("ts") * 1000;
                        HashSet<String> orderSet = new HashSet<>();
                        orderSet.add(value.getString("order_id"));
                        HashSet<String> userSet = new HashSet<>();
                        userSet.add(value.getString("user_id"));
                        TradeSourceOrderBean bean = TradeSourceOrderBean.builder()
                                .orderDetailId(ctx.getCurrentKey())
                                .sourceId(value.getString("sc"))
                                .finalAmount(value.getBigDecimal("final_amount"))
                                .orderSet(orderSet)
                                .userSet(userSet)
                                .ts(ts)
                                .build();

                        TradeSourceOrderBean lastBean = lastBeanState.value();
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
