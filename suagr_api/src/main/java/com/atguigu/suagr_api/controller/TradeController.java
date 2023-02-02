package com.atguigu.suagr_api.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.suagr_api.bean.SourceOrder;
import com.atguigu.suagr_api.service.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@RestController
public class TradeController {
    @Autowired
    TradeService tradeService;

    @RequestMapping("/sugar/orderBySource")
    public String orderBySource(Integer date){
        if (date == null){
            date =Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
        System.out.println(date);

        List<SourceOrder> list = tradeService.orderBySource(date);
        System.out.println(list);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("status", 0);
        jsonObject.put("msg", "");

        JSONArray data = new JSONArray();

        for (SourceOrder so : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", so.getSource_name());
            obj.put("value", so.getFinal_amount());
            data.add(obj);
        }

        jsonObject.put("data", data);
        System.out.println(jsonObject.toJSONString());
        return jsonObject.toJSONString();
    }
}
