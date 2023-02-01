package com.atguigu.gmall_edu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TrafficSourcePageViewBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 来源
    private String source;

    // 来源名称
    private String sourceName;

    // 新老用户标记
    private String isNew;

    // 独立访客数
    private Long uvCt;

    // 会话数
    private Long svCt;

    // 浏览页面数
    private Long pvCt;

    // 累积访问时长
    private Long durSum;

    // 时间戳
    private Long ts;
}
