package com.atguigu.suagr_api.mapper;

import com.atguigu.suagr_api.bean.SourceOrder;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface TradeMapper {
    @Select("select source_name,sum(final_amount) final_amount " +
            "from gmall_edu.dws_trade_source_order_window " +
            "where toYYYYMMDD(stt) = #{date} " +
            "group by source_name")
    List<SourceOrder> orderBySource(@Param("date") int date);
}
