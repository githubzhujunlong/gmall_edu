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
public class ExamPaperBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 试卷id
    private String paperId;

    // 试卷名称
    private String paperName;

    // 课程id
    private String courseId;

    // 课程名称
    private String courseName;

    // 学科id
    private String subjectId;

    // 学科名称
    private String subjectName;

    // 分类id
    private String categoryId;

    // 分类名称
    private String categoryName;

    // 分数
    @NotSink
    private Double score;

    // 平均分
    private BigDecimal avgScore;

    // 考试人数
    @NotSink
    private HashSet<String> userSet;
    private Long userCount;

    // 时间
    @NotSink
    @Builder.Default
    private Long durSec = 0L;

    // 平均时间
    private BigDecimal avgDurSec;

    // 时间戳
    private Long ts;
}
