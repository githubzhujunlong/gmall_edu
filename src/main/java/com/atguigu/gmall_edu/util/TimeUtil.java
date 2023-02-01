package com.atguigu.gmall_edu.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TimeUtil {
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String tsToDate(Long ts){
        return dateFormatter.format(LocalDateTime.ofInstant(new Date(ts).toInstant(), ZoneId.systemDefault()));
    }

    public static String tsToDateTime(Long ts){
        return dateTimeFormatter.format(LocalDateTime.ofInstant(new Date(ts).toInstant(), ZoneId.systemDefault()));
    }

    public static long dateToTs(String date) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime();
    }

    public static long dateTimeToTs(String date) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime();
    }

    public static void main(String[] args) throws ParseException {
        System.out.println(dateToTs("2023-02-01"));
    }
}
