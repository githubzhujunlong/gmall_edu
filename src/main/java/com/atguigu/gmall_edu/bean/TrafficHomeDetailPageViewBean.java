package com.atguigu.gmall_edu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TrafficHomeDetailPageViewBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 首页访客数
    @Builder.Default
    private Long homeCount = 0L;

    // 详情页访客数
    @Builder.Default
    private Long detailCount = 0L;

    // 时间戳
    private Long ts;
}
