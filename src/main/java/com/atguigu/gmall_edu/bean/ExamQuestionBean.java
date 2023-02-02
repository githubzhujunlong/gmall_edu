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
public class ExamQuestionBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 问题id
    private String questionId;

    // 问题名称
    private String questionName;

    // 答题次数
    private Long answerCount;

    // 正确答题次数
    private Long correctAnswerCount;

    // 答题独立用户数
    @NotSink
    private HashSet<String> answerUserSet;
    private Long answerUserCount;

    // 正确答题独立用户数
    @NotSink
    private HashSet<String> correctAnswerUserSet;
    private Long correctAnswerUserCount;

    // 时间戳
    private Long ts;
}
