package com.atguigu.gmall_edu.function;

import com.atguigu.gmall_edu.util.SqlUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

@FunctionHint(output = @DataTypeHint("row<keyword string>"))
public class KwSplit extends TableFunction<Row> {
    public void eval(String item){
        Set<String> keywords = SqlUtil.ikSplit(item);
        for (String keyword : keywords) {
            collect(Row.of(keyword));
        }
    }
}
