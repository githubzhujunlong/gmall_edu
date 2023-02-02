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
public class TradeSourceOrderBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 用于去重
    @NotSink
    private String orderDetailId;

    // 省份id
    private String sourceId;

    // 省份名称
    private String sourceName;

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
