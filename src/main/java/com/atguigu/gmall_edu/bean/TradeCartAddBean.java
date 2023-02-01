package com.atguigu.gmall_edu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradeCartAddBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 课程id
    private String courseId;

    // 课程名称
    private String courseName;

    // 学科id
    @Builder.Default
    private String subjectId = "";

    // 学科名称
    private String subjectName;

    // 分类id
    private String categoryId;

    // 分类名称
    private String categoryName;

    // 加购人数
    private Long cartCount;

    // 时间戳
    private Long ts;
}
