package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.TrafficSourcePageViewBean;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.function.AsyncDimFunction;
import com.atguigu.gmall_edu.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DWS_01_DWSTrafficSourcePageView extends BaseApp {
    public static void main(String[] args) {
        new DWS_01_DWSTrafficSourcePageView().init(
                3341,
                2,
                "DWS_01_DWSTrafficSourcePageView",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 解析成pojo
        SingleOutputStreamOperator<TrafficSourcePageViewBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        // 开窗聚合
        SingleOutputStreamOperator<TrafficSourcePageViewBean> AggStream = windowAndAgg(parseStream);
        //AggStream.print();

        // 补充维度
        SingleOutputStreamOperator<TrafficSourcePageViewBean> dimStream = joinDims(AggStream);
        //dimStream.print();

        // 写入clickhouse
        writeToClickhouse(dimStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<TrafficSourcePageViewBean> aggStream) {
        aggStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_traffic_source_page_view_window",
                TrafficSourcePageViewBean.class
        ));
    }

    private SingleOutputStreamOperator<TrafficSourcePageViewBean> joinDims(SingleOutputStreamOperator<TrafficSourcePageViewBean> aggStream) {
        return AsyncDataStream.unorderedWait(
                aggStream,
                new AsyncDimFunction<TrafficSourcePageViewBean>() {
                    @Override
                    public String getTable() {
                        return "dim_base_source";
                    }

                    @Override
                    public String getId(TrafficSourcePageViewBean input) {
                        return input.getSource();
                    }

                    @Override
                    public void joinDim(TrafficSourcePageViewBean input, JSONObject dim) {
                        input.setSourceName(dim.getString("SOURCE_SITE"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

    }

    private SingleOutputStreamOperator<TrafficSourcePageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficSourcePageViewBean> parseStream) {
        return parseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TrafficSourcePageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(bean -> bean.getSource() + ":" + bean.getIsNew())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TrafficSourcePageViewBean>() {
                    @Override
                    public TrafficSourcePageViewBean reduce(TrafficSourcePageViewBean value1, TrafficSourcePageViewBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                }, new WindowFunction<TrafficSourcePageViewBean, TrafficSourcePageViewBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TrafficSourcePageViewBean> input, Collector<TrafficSourcePageViewBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        TrafficSourcePageViewBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TrafficSourcePageViewBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(json -> json.getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, TrafficSourcePageViewBean>() {

                    // 记录上次访问时间
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDateState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<String>(
                                        "lastVisitDate",
                                        String.class
                                ));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<TrafficSourcePageViewBean> out) throws Exception {
                        Long ts = value.getLong("ts");
                        String today = TimeUtil.tsToDate(ts);
                        TrafficSourcePageViewBean bean = TrafficSourcePageViewBean.builder()
                                .source(value.getString("sc"))
                                .isNew(value.getString("is_new"))
                                .ts(ts)
                                .build();

                        Long uvCt = 0L;
                        String lastVisitDate = lastVisitDateState.value();
                        // 今天与状态中的日期不同则为该日的独立访客
                        if (!today.equals(lastVisitDate)){
                            uvCt = 1L;
                            lastVisitDateState.update(today);
                        }
                        bean.setUvCt(uvCt);

                        // 上个页面为空则视为开启了一个新的会话
                        Long svCt = 0L;
                        if (value.getString("last_page_id") == null){
                            svCt = 1L;
                        }
                        bean.setSvCt(svCt);

                        Long during_time = value.getLong("during_time");

                        bean.setPvCt(1L);
                        bean.setDurSum(during_time);
                        out.collect(bean);
                    }
                });
    }
}
