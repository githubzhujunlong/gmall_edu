package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.ExamPaperBean;
import com.atguigu.gmall_edu.bean.ExamQuestionBean;
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

public class DWS_13_DWSExamQuestion extends BaseApp {
    public static void main(String[] args) {
        new DWS_13_DWSExamQuestion().init(
                3352,
                2,
                "DWS_12_DWSExamPaper",
                Constant.TOPIC_DWD_EXAM_EXAM_QUESTION
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<ExamQuestionBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<ExamQuestionBean> aggStream = windowAndAgg(parseStream);
        //aggStream.print();

        SingleOutputStreamOperator<ExamQuestionBean> dimStream = joinDims(aggStream);
        //dimStream.print();

        writeToClickhouse(dimStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<ExamQuestionBean> dimStream) {
        dimStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_exam_question_window",
                ExamQuestionBean.class
        ));
    }

    private SingleOutputStreamOperator<ExamQuestionBean> joinDims(SingleOutputStreamOperator<ExamQuestionBean> aggStream) {
        return AsyncDataStream.unorderedWait(
                aggStream,
                new AsyncDimFunction<ExamQuestionBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_TEST_QUESTION_INFO";
                    }

                    @Override
                    public String getId(ExamQuestionBean input) {
                        return input.getQuestionId();
                    }

                    @Override
                    public void joinDim(ExamQuestionBean input, JSONObject dim) {
                        input.setQuestionName(dim.getString("QUESTION_TXT"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<ExamQuestionBean> windowAndAgg(SingleOutputStreamOperator<ExamQuestionBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ExamQuestionBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(ExamQuestionBean::getQuestionId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<ExamQuestionBean>() {
                    @Override
                    public ExamQuestionBean reduce(ExamQuestionBean value1, ExamQuestionBean value2) throws Exception {
                        value1.setAnswerCount(value1.getAnswerCount() + value2.getAnswerCount());
                        value1.setCorrectAnswerCount(value1.getCorrectAnswerCount() + value2.getCorrectAnswerCount());
                        value1.getAnswerUserSet().addAll(value2.getAnswerUserSet());
                        value1.getCorrectAnswerUserSet().addAll(value2.getCorrectAnswerUserSet());
                        return value1;
                    }
                }, new WindowFunction<ExamQuestionBean, ExamQuestionBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<ExamQuestionBean> input, Collector<ExamQuestionBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        ExamQuestionBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        bean.setAnswerUserCount((long) bean.getAnswerUserSet().size());
                        bean.setCorrectAnswerUserCount((long) bean.getCorrectAnswerUserSet().size());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<ExamQuestionBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(line -> {
                    JSONObject jsonObject = JSON.parseObject(line);

                    Long correctAnswerCount = 0L;
                    if ("1".equals(jsonObject.getString("is_correct"))){
                        correctAnswerCount = 1L;
                    }

                    String user_id = jsonObject.getString("user_id");

                    HashSet<String> answerUserSet = new HashSet<>();
                    answerUserSet.add(user_id);

                    HashSet<String> correctAnswerUserSet = new HashSet<>();
                    if (correctAnswerCount == 1){
                        correctAnswerUserSet.add(user_id);
                    }

                    ExamQuestionBean bean = ExamQuestionBean.builder()
                            .questionId(jsonObject.getString("question_id"))
                            .answerCount(1L)
                            .correctAnswerCount(correctAnswerCount)
                            .answerUserSet(answerUserSet)
                            .correctAnswerUserSet(correctAnswerUserSet)
                            .ts(jsonObject.getLong("ts") * 1000)
                            .build();

                    return bean;
                });

    }
}
