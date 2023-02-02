package com.atguigu.gmall_edu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_edu.app.BaseApp;
import com.atguigu.gmall_edu.bean.ExamQuestionBean;
import com.atguigu.gmall_edu.bean.VideoChapterBean;
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
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DWS_14_DWSPlayVideoChapter extends BaseApp {
    public static void main(String[] args) {
        new DWS_14_DWSPlayVideoChapter().init(
                3353,
                2,
                "DWS_14_DWSPlayVideoChapter",
                Constant.TOPIC_DWD_TRAFFIC_APPVIDEO
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<VideoChapterBean> parseStream = parseToPojo(stream);
        //parseStream.print();

        SingleOutputStreamOperator<VideoChapterBean> dimStream = joinDims(parseStream);
        //dimStream.print();

        SingleOutputStreamOperator<VideoChapterBean> aggStream = windowAndAgg(dimStream);
        //aggStream.print();

        writeToClickhouse(aggStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<VideoChapterBean> aggStream) {
        aggStream.addSink(FlinkSinkUtil.getClickhouseSink(
                "dws_play_video_chapter_window",
                VideoChapterBean.class
        ));
    }

    private SingleOutputStreamOperator<VideoChapterBean> windowAndAgg(SingleOutputStreamOperator<VideoChapterBean> dimStream) {
        return dimStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VideoChapterBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(VideoChapterBean::getChapterId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<VideoChapterBean>() {
                    @Override
                    public VideoChapterBean reduce(VideoChapterBean value1, VideoChapterBean value2) throws Exception {
                        value1.getSidSet().addAll(value2.getSidSet());
                        value1.setPlaySec(value1.getPlaySec() + value2.getPlaySec());
                        value1.getUserSet().addAll(value2.getUserSet());
                        return value1;
                    }
                }, new WindowFunction<VideoChapterBean, VideoChapterBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<VideoChapterBean> input, Collector<VideoChapterBean> out) throws Exception {
                        String stt = TimeUtil.tsToDateTime(window.getStart());
                        String edt = TimeUtil.tsToDateTime(window.getEnd());

                        VideoChapterBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setTs(System.currentTimeMillis());
                        bean.setPlayCount((long) bean.getSidSet().size());
                        bean.setUserCount((long) bean.getUserSet().size());
                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<VideoChapterBean> joinDims(SingleOutputStreamOperator<VideoChapterBean> parseStream) {
        SingleOutputStreamOperator<VideoChapterBean> video = AsyncDataStream.unorderedWait(
                parseStream,
                new AsyncDimFunction<VideoChapterBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_VIDEO_INFO";
                    }

                    @Override
                    public String getId(VideoChapterBean input) {
                        return input.getVideoId();
                    }

                    @Override
                    public void joinDim(VideoChapterBean input, JSONObject dim) {
                        input.setChapterId(dim.getString("CHAPTER_ID"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        return AsyncDataStream.unorderedWait(
                video,
                new AsyncDimFunction<VideoChapterBean>() {
                    @Override
                    public String getTable() {
                        return "DIM_CHAPTER_INFO";
                    }

                    @Override
                    public String getId(VideoChapterBean input) {
                        return input.getChapterId();
                    }

                    @Override
                    public void joinDim(VideoChapterBean input, JSONObject dim) {
                        input.setChapterName(dim.getString("CHAPTER_NAME"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<VideoChapterBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(line -> {
                    JSONObject jsonObject = JSON.parseObject(line);

                    HashSet<String> sidSet = new HashSet<>();
                    sidSet.add(jsonObject.getString("sid"));

                    HashSet<String> userSet = new HashSet<>();
                    userSet.add(jsonObject.getString("user_id"));

                    VideoChapterBean bean = VideoChapterBean.builder()
                            .videoId(jsonObject.getString("video_id"))
                            .sidSet(sidSet)
                            .playSec(jsonObject.getLong("play_sec"))
                            .UserSet(userSet)
                            .ts(jsonObject.getLong("ts"))
                            .build();

                    return bean;
                });

    }
}
