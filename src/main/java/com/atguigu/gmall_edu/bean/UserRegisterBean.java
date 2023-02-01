package com.atguigu.gmall_edu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserRegisterBean {
    // 窗口开始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 登录用户数
    private Long userRegisterCount;

    // 时间戳
    private Long ts;
}
