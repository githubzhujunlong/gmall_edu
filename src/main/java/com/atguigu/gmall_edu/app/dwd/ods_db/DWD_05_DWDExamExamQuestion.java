package com.atguigu.gmall_edu.app.dwd.ods_db;

import com.atguigu.gmall_edu.app.BaseSqlApp;
import com.atguigu.gmall_edu.common.Constant;
import com.atguigu.gmall_edu.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DWD_05_DWDExamExamQuestion extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD_05_DWDExamExamQuestion().init(
                3339,
                2,
                "DWD_05_DWDExamExamQuestion"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        readOdsDb(tableEnv, "DWD_05_DWDExamExamQuestion");

        // 过滤测验问题表
        Table testExamQuestion = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['exam_id'] exam_id, " +
                "data['paper_id'] paper_id, " +
                "data['question_id'] question_id, " +
                "data['user_id'] user_id, " +
                "data['is_correct'] is_correct, " +
                "data['score'] score, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from ods_db " +
                "where `database` = 'gmall_edu' " +
                "and `table` = 'test_exam_question' " +
                "and type = 'insert' ");
        tableEnv.createTemporaryView("test_exam_question", testExamQuestion);

        // 过滤测验表
        Table testExam = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['score'] score, " +
                "data['duration_sec'] duration_sec, " +
                "data['create_time'] create_time " +
                "from ods_db " +
                "where `database` = 'gmall_edu' " +
                "and `table` = 'test_exam' " +
                "and type = 'insert' ");
        tableEnv.createTemporaryView("test_exam", testExam);

        // join
        Table result = tableEnv.sqlQuery("select " +
                "teq.id, " +
                "teq.exam_id, " +
                "paper_id, " +
                "question_id, " +
                "user_id, " +
                "is_correct, " +
                "teq.score question_score, " +
                "te.score exam_score, " +
                "duration_sec, " +
                "teq.create_time, " +
                "ts " +
                "from test_exam_question teq " +
                "join test_exam te " +
                "on teq.exam_id = te.id ");

        // 写入kafka
        tableEnv.executeSql("create table dwd_exam_exam_question(" +
                "id string, " +
                "exam_id string, " +
                "paper_id string, " +
                "question_id string, " +
                "user_id string, " +
                "is_correct string, " +
                "question_score string, " +
                "exam_score string, " +
                "duration_sec string, " +
                "create_time string, " +
                "ts bigint " +
                ")" + SqlUtil.getKafkaSinkConnector(Constant.TOPIC_DWD_EXAM_EXAM_QUESTION));
        result.executeInsert("dwd_exam_exam_question");
    }
}
