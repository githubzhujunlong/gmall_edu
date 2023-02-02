package com.atguigu.suagr_api.service;

import com.atguigu.suagr_api.bean.SourceOrder;
import com.atguigu.suagr_api.mapper.TradeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TradeServiceImpl implements TradeService{

    @Autowired
    TradeMapper tradeMapper;

    @Override
    public List<SourceOrder> orderBySource(int date) {
       return tradeMapper.orderBySource(date);
    }
}
