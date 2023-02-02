package com.atguigu.suagr_api.service;

import com.atguigu.suagr_api.bean.SourceOrder;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Service;

import java.util.List;

public interface TradeService {
    List<SourceOrder> orderBySource(int date);
}
