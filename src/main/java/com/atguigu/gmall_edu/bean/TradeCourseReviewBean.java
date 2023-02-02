package com.atguigu.gmall_edu.bean;

import com.atguigu.gmall_edu.annotation.NotSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.HashSet;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradeCourseReviewBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 课程id
    private String courseId;

    // 课程名称
    private String courseName;

    // 评分
    @NotSink
    private Integer score;

    // 平均分
    private BigDecimal avgScore;

    // 评价人数
    @NotSink
    private HashSet<String> userSet;
    private Long userCount;

    // 好评数
    @NotSink
    @Builder.Default
    private Long goodCount = 0L;

    // 好评率
    private BigDecimal goodRate;

    // 时间戳
    private Long ts;
}
