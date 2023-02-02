package com.atguigu.gmall_edu.bean;

import com.atguigu.gmall_edu.annotation.NotSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradeCourseOrderBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 用于去重
    @NotSink
    private String orderDetailId;

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

    // 下单次数
    @NotSink
    private HashSet<String> orderSet;
    private Long orderCount;

    // 下单人数
    @NotSink
    private HashSet<String> userSet;
    private Long userCount;

    // 下单金额
    private BigDecimal finalAmount;

    // 时间戳
    private Long ts;
}
