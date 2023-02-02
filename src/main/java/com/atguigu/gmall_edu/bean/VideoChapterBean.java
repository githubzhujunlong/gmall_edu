package com.atguigu.gmall_edu.bean;

import com.atguigu.gmall_edu.annotation.NotSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VideoChapterBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 视频id
    @NotSink
    private String videoId;

    // 章节id
    private String chapterId;

    // 章节名称
    private String chapterName;

    // 视频播放次数
    @NotSink
    private HashSet<String> sidSet;
    private Long playCount;

    // 累积播放时长
    private Long playSec;

    // 观看人数
    @NotSink
    private HashSet<String> UserSet;
    private Long UserCount;

    // 时间戳
    private Long ts;
}
