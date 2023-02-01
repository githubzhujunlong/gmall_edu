package com.atguigu.gmall_edu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TrafficKeyWordBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 关键词
    private String keyword;

    // 关键词出现的次数
    private Long keyword_count;

    // 时间戳
    private Long ts;
}
