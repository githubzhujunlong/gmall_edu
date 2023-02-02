package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.TradeCourseOrderBean;
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

public class DWS_07_DWSTradeCourseOrder extends BaseApp {
    public static void main(String[] args) {
        new DWS_07_DWSTradeCourseOrder().init(
                3347,
                2,
                "DWS_07_DWSTradeCourseOrder",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeCourseOrderBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<TradeCourseOrderBean> aggStream = windowAndAgg(parseStream);
        //aggStream.print();

        SingleOutputStreamOperator<TradeCourseOrderBean> dimStream = joinDims(aggStream);
        //dimStream.print();

        writeToClickhouse(dimStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<TradeCourseOrderBean> dimStream) {
        dimStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_trade_course_order_window",
                TradeCourseOrderBean.class
        ));
    }

    private SingleOutputStreamOperator<TradeCourseOrderBean> joinDims(SingleOutputStreamOperator<TradeCourseOrderBean> aggStream) {
        SingleOutputStreamOperator<TradeCourseOrderBean> course = AsyncDataStream.unorderedWait(
                aggStream,
                new AsyncDimFunction<TradeCourseOrderBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_COURSE_INFO";
                    }

                    @Override
                    public String getId(TradeCourseOrderBean input) {
                        return input.getCourseId();
                    }

                    @Override
                    public void joinDim(TradeCourseOrderBean input, JSONObject dim) {
                        input.setSubjectId(dim.getString("SUBJECT_ID"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeCourseOrderBean> subject = AsyncDataStream.unorderedWait(
                course,
                new AsyncDimFunction<TradeCourseOrderBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_BASE_SUBJECT_INFO";
                    }

                    @Override
                    public String getId(TradeCourseOrderBean input) {
                        return input.getSubjectId();
                    }

                    @Override
                    public void joinDim(TradeCourseOrderBean input, JSONObject dim) {
                        input.setSubjectName(dim.getString("SUBJECT_NAME"));
                        input.setCategoryId(dim.getString("CATEGORY_ID"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        return AsyncDataStream.unorderedWait(
                subject,
                new AsyncDimFunction<TradeCourseOrderBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_BASE_CATEGORY_INFO";
                    }

                    @Override
                    public String getId(TradeCourseOrderBean input) {
                        return input.getCategoryId();
                    }

                    @Override
                    public void joinDim(TradeCourseOrderBean input, JSONObject dim) {
                        input.setCategoryName(dim.getString("CATEGORY_NAME"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<TradeCourseOrderBean> windowAndAgg(SingleOutputStreamOperator<TradeCourseOrderBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeCourseOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(TradeCourseOrderBean::getCourseId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeCourseOrderBean>() {
                    @Override
                    public TradeCourseOrderBean reduce(TradeCourseOrderBean value1, TradeCourseOrderBean value2) throws Exception {
                        value1.getOrderSet().addAll(value2.getOrderSet());
                        value1.getUserSet().addAll(value2.getUserSet());
                        value1.setFinalAmount(value1.getFinalAmount().add(value2.getFinalAmount()));
                        return value1;
                    }
                }, new WindowFunction<TradeCourseOrderBean, TradeCourseOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeCourseOrderBean> input, Collector<TradeCourseOrderBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        TradeCourseOrderBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        bean.setOrderCount((long) bean.getOrderSet().size());
                        bean.setUserCount((long) bean.getUserSet().size());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TradeCourseOrderBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(json -> json.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeCourseOrderBean>() {

                    private ValueState<TradeCourseOrderBean> lastBeanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastBeanState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<TradeCourseOrderBean>(
                                        "lastBeanState",
                                        TradeCourseOrderBean.class
                                ));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<TradeCourseOrderBean> out) throws Exception {
                        long ts = value.getLong("ts") * 1000;
                        HashSet<String> orderSet = new HashSet<>();
                        orderSet.add(value.getString("order_id"));
                        HashSet<String> userSet = new HashSet<>();
                        userSet.add(value.getString("user_id"));
                        TradeCourseOrderBean bean = TradeCourseOrderBean.builder()
                                .orderDetailId(ctx.getCurrentKey())
                                .courseId(value.getString("course_id"))
                                .courseName(value.getString("course_name"))
                                .finalAmount(value.getBigDecimal("final_amount"))
                                .orderSet(orderSet)
                                .userSet(userSet)
                                .ts(ts)
                                .build();

                        TradeCourseOrderBean lastBean = lastBeanState.value();
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
