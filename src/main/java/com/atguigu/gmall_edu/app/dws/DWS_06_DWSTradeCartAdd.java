package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.TradeCartAddBean;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.function.AsyncDimFunction;
import com.atguigu.gmall_edu.util.FlinkSinkUtil;
import com.atguigu.gmall_edu.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DWS_06_DWSTradeCartAdd extends BaseApp {
    public static void main(String[] args) {
        new DWS_06_DWSTradeCartAdd().init(
                3346,
                2,
                "DWS_06_DWSTradeCartAdd",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeCartAddBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<TradeCartAddBean> aggStream = windowAndAgg(parseStream);
        //aggStream.print();

        SingleOutputStreamOperator<TradeCartAddBean> dimStream = joinDims(aggStream);
        //dimStream.print();

        writeToClickhouse(dimStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<TradeCartAddBean> aggStream) {
        aggStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_trade_cart_add_window",
                TradeCartAddBean.class
        ));
    }

    private SingleOutputStreamOperator<TradeCartAddBean> joinDims(SingleOutputStreamOperator<TradeCartAddBean> aggStream) {
        SingleOutputStreamOperator<TradeCartAddBean> course = AsyncDataStream.unorderedWait(
                aggStream,
                new AsyncDimFunction<TradeCartAddBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_COURSE_INFO";
                    }

                    @Override
                    public String getId(TradeCartAddBean input) {
                        return input.getCourseId();
                    }

                    @Override
                    public void joinDim(TradeCartAddBean input, JSONObject dim) {
                        input.setSubjectId(dim.getString("SUBJECT_ID"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeCartAddBean> subject = AsyncDataStream.unorderedWait(
                course,
                new AsyncDimFunction<TradeCartAddBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_BASE_SUBJECT_INFO";
                    }

                    @Override
                    public String getId(TradeCartAddBean input) {
                        return input.getSubjectId();
                    }

                    @Override
                    public void joinDim(TradeCartAddBean input, JSONObject dim) {
                        input.setSubjectName(dim.getString("SUBJECT_NAME"));
                        input.setCategoryId(dim.getString("CATEGORY_ID"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        return AsyncDataStream.unorderedWait(
                subject,
                new AsyncDimFunction<TradeCartAddBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_BASE_CATEGORY_INFO";
                    }

                    @Override
                    public String getId(TradeCartAddBean input) {
                        return input.getCategoryId();
                    }

                    @Override
                    public void joinDim(TradeCartAddBean input, JSONObject dim) {
                        input.setCategoryName(dim.getString("CATEGORY_NAME"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<TradeCartAddBean> windowAndAgg(SingleOutputStreamOperator<TradeCartAddBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeCartAddBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(TradeCartAddBean::getCourseId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeCartAddBean>() {
                    @Override
                    public TradeCartAddBean reduce(TradeCartAddBean value1, TradeCartAddBean value2) throws Exception {
                        value1.setCartCount(value1.getCartCount() + value2.getCartCount());
                        return value1;
                    }
                }, new WindowFunction<TradeCartAddBean, TradeCartAddBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeCartAddBean> input, Collector<TradeCartAddBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        TradeCartAddBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TradeCartAddBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(line -> {
                    JSONObject jsonObject = JSON.parseObject(line);
                    String course_id = jsonObject.getString("course_id");
                    String course_name = jsonObject.getString("course_name");
                    long ts = jsonObject.getLong("ts") * 1000;
                    TradeCartAddBean bean = TradeCartAddBean.builder()
                            .courseId(course_id)
                            .courseName(course_name)
                            .cartCount(1L)
                            .ts(ts)
                            .build();
                    return bean;
                });
    }
}
