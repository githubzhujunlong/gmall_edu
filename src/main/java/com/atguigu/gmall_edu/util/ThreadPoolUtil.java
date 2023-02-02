package com.atguigu.gmall_edu.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    public static ThreadPoolExecutor getThreadPool(){
        return new ThreadPoolExecutor(
                300,
                600,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>()
        );
    }
}
