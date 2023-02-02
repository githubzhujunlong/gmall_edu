package com.atguigu.suagr_api.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceOrder {
    private String source_name;
    private BigDecimal final_amount;
}
