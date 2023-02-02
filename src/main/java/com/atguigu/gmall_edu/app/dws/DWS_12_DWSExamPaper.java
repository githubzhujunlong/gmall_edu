package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.ExamPaperBean;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DWS_12_DWSExamPaper extends BaseApp {
    public static void main(String[] args) {
        new DWS_12_DWSExamPaper().init(
                3352,
                2,
                "DWS_12_DWSExamPaper",
                Constant.TOPIC_DWD_EXAM_EXAM_QUESTION
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<ExamPaperBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<ExamPaperBean> aggStream = windowAndAgg(parseStream);
        //aggStream.print();

        SingleOutputStreamOperator<ExamPaperBean> dimStream = joinDims(aggStream);
        //dimStream.print();

        writeToClickhouse(dimStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<ExamPaperBean> dimStream) {
        dimStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_exam_paper_window",
                ExamPaperBean.class
        ));
    }

    private SingleOutputStreamOperator<ExamPaperBean> joinDims(SingleOutputStreamOperator<ExamPaperBean> aggStream) {
        SingleOutputStreamOperator<ExamPaperBean> paper = AsyncDataStream.unorderedWait(
                aggStream,
                new AsyncDimFunction<ExamPaperBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_TEST_PAPER";
                    }

                    @Override
                    public String getId(ExamPaperBean input) {
                        return input.getPaperId();
                    }

                    @Override
                    public void joinDim(ExamPaperBean input, JSONObject dim) {
                        input.setPaperName(dim.getString("PAPER_TITLE"));
                        input.setCourseId(dim.getString("COURSE_ID"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<ExamPaperBean> course = AsyncDataStream.unorderedWait(
                paper,
                new AsyncDimFunction<ExamPaperBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_COURSE_INFO";
                    }

                    @Override
                    public String getId(ExamPaperBean input) {
                        return input.getCourseId();
                    }

                    @Override
                    public void joinDim(ExamPaperBean input, JSONObject dim) {
                        input.setCourseName(dim.getString("COURSE_NAME"));
                        input.setSubjectId(dim.getString("SUBJECT_ID"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<ExamPaperBean> subject = AsyncDataStream.unorderedWait(
                course,
                new AsyncDimFunction<ExamPaperBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_BASE_SUBJECT_INFO";
                    }

                    @Override
                    public String getId(ExamPaperBean input) {
                        return input.getSubjectId();
                    }

                    @Override
                    public void joinDim(ExamPaperBean input, JSONObject dim) {
                        input.setSubjectName(dim.getString("SUBJECT_NAME"));
                        input.setCategoryId(dim.getString("CATEGORY_ID"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        return AsyncDataStream.unorderedWait(
                subject,
                new AsyncDimFunction<ExamPaperBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_BASE_CATEGORY_INFO";
                    }

                    @Override
                    public String getId(ExamPaperBean input) {
                        return input.getCategoryId();
                    }

                    @Override
                    public void joinDim(ExamPaperBean input, JSONObject dim) {
                        input.setCategoryName(dim.getString("CATEGORY_NAME"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<ExamPaperBean> windowAndAgg(SingleOutputStreamOperator<ExamPaperBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ExamPaperBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(ExamPaperBean::getPaperId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<ExamPaperBean>() {
                    @Override
                    public ExamPaperBean reduce(ExamPaperBean value1, ExamPaperBean value2) throws Exception {
                        value1.getUserSet().addAll(value2.getUserSet());
                        value1.setScore(value1.getScore() + value2.getScore());
                        value1.setDurSec(value1.getDurSec() + value2.getDurSec());
                        return value1;
                    }
                }, new WindowFunction<ExamPaperBean, ExamPaperBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<ExamPaperBean> input, Collector<ExamPaperBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        ExamPaperBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        bean.setUserCount((long) bean.getUserSet().size());
                        bean.setAvgScore(BigDecimal.valueOf(bean.getScore() / bean.getUserCount()));
                        bean.setAvgDurSec(BigDecimal.valueOf(bean.getDurSec() / bean.getUserCount()));
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<ExamPaperBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(json -> json.getString("exam_id"))
                .process(new KeyedProcessFunction<String, JSONObject, ExamPaperBean>() {

                    private ValueState<ExamPaperBean> lastBeanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastBeanState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<ExamPaperBean>(
                                        "lastBeanState",
                                        ExamPaperBean.class
                                ));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<ExamPaperBean> out) throws Exception {
                        ExamPaperBean lastBean = lastBeanState.value();
                        HashSet<String> userSet = new HashSet<>();
                        userSet.add(value.getString("user_id"));
                        ExamPaperBean bean = ExamPaperBean.builder()
                                .paperId(value.getString("paper_id"))
                                .score(value.getDouble("exam_score"))
                                .userSet(userSet)
                                .durSec(value.getLong("duration_sec"))
                                .ts(value.getLong("ts") * 1000)
                                .build();

                        if (lastBean == null){
                            out.collect(bean);
                        }else {
                            lastBean.setScore(bean.getScore() - lastBean.getScore());
                            lastBean.setDurSec(bean.getDurSec() - lastBean.getDurSec());
                            out.collect(lastBean);
                        }

                        lastBeanState.update(bean);
                    }
                });

    }
}
