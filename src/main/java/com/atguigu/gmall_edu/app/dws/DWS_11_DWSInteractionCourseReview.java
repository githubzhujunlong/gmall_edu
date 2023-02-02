package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.TradeCourseReviewBean;
import com.atguigu.gmall_edu.bean.TradePaySucBean;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DWS_11_DWSInteractionCourseReview extends BaseApp {
    public static void main(String[] args) {
        new DWS_11_DWSInteractionCourseReview().init(
                3351,
                2,
                "DWS_11_DWSInteractionCourseReview",
                Constant.TOPIC_DWD_INTERACTION_COMMENT
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeCourseReviewBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<TradeCourseReviewBean> aggStream = windowAndAgg(parseStream);
        //aggStream.print();

        SingleOutputStreamOperator<TradeCourseReviewBean> dimStream = joinDims(aggStream);
        //dimStream.print();

        writeToClickhouse(dimStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<TradeCourseReviewBean> dimStream) {
        dimStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_interaction_course_review_window",
                TradeCourseReviewBean.class
        ));
    }

    private SingleOutputStreamOperator<TradeCourseReviewBean> joinDims(SingleOutputStreamOperator<TradeCourseReviewBean> aggStream) {
        return AsyncDataStream.unorderedWait(
                aggStream,
                new AsyncDimFunction<TradeCourseReviewBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_COURSE_INFO";
                    }

                    @Override
                    public String getId(TradeCourseReviewBean input) {
                        return input.getCourseId();
                    }

                    @Override
                    public void joinDim(TradeCourseReviewBean input, JSONObject dim) {
                        input.setCourseName(dim.getString("COURSE_NAME"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<TradeCourseReviewBean> windowAndAgg(SingleOutputStreamOperator<TradeCourseReviewBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeCourseReviewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(TradeCourseReviewBean::getCourseId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeCourseReviewBean>() {
                    @Override
                    public TradeCourseReviewBean reduce(TradeCourseReviewBean value1, TradeCourseReviewBean value2) throws Exception {
                        value1.setScore(value1.getScore() + value2.getScore());
                        value1.getUserSet().addAll(value2.getUserSet());
                        value1.setGoodCount(value1.getGoodCount() + value2.getGoodCount());
                        return value1;
                    }
                }, new WindowFunction<TradeCourseReviewBean, TradeCourseReviewBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeCourseReviewBean> input, Collector<TradeCourseReviewBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        TradeCourseReviewBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        bean.setUserCount((long) bean.getUserSet().size());
                        bean.setAvgScore(new BigDecimal(bean.getScore() / bean.getUserCount()));
                        bean.setGoodRate(new BigDecimal(bean.getGoodCount() / bean.getUserCount()));
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TradeCourseReviewBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(line -> {
                    JSONObject json = JSON.parseObject(line);
                    HashSet<String> userSet = new HashSet<>();
                    userSet.add(json.getString("user_id"));
                    Long ts = json.getLong("create_time");
                    Integer review_stars = json.getInteger("review_stars");
                    TradeCourseReviewBean bean = TradeCourseReviewBean.builder()
                            .courseId(json.getString("course_id"))
                            .userSet(userSet)
                            .score(review_stars)
                            .ts(ts)
                            .build();
                    if (review_stars == 5){
                        bean.setGoodCount(1L);
                    }
                    return bean;
                });


    }


}
