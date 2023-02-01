package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.TrafficHomeDetailPageViewBean;
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


public class DWS_03_DWSTrafficHomeDetailPageView extends BaseApp {
    public static void main(String[] args) {
        new DWS_03_DWSTrafficHomeDetailPageView().init(
                3345,
                2,
                "DWS_03_DWSTrafficHomeDetailPageView",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> aggStream = windowAndAgg(parseStream);
        //aggStream.print();

        writeToClickhouse(aggStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> aggStream) {
        aggStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_user_traffic_home_detail_page_view_window",
                TrafficHomeDetailPageViewBean.class
        ));
    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeCount(value1.getHomeCount() + value2.getHomeCount());
                        value1.setDetailCount(value1.getDetailCount() + value2.getDetailCount());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        TrafficHomeDetailPageViewBean bean = values.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                // 过滤主页和详情页日志
                .filter(json -> "home".equals(json.getString("page_id")) || "course_detail".equals(json.getString("page_id")))
                .map(json -> {
                    String page_id = json.getString("page_id");
                    Long ts = json.getLong("ts");
                    TrafficHomeDetailPageViewBean bean = TrafficHomeDetailPageViewBean.builder()
                            .ts(ts)
                            .build();

                    if ("home".equals(page_id)){
                        bean.setHomeCount(1L);
                    }else {
                        bean.setDetailCount(1L);
                    }

                    return bean;
                });
    }
}
