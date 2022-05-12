package com.ccy.core;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ThreadLocalDateUtils {
    private static final String dateFormat = "yyyyMMdd HH";
    private static ThreadLocal<DateFormat> threadLocal = new ThreadLocal<DateFormat>();
    public static DateFormat getDateFormat()
    {
        DateFormat df = threadLocal.get();
        if(df==null){
            df = new SimpleDateFormat(dateFormat);
            threadLocal.set(df);
        }
        return df;
    }

    public static String formatDate(Date date) throws ParseException {
        return getDateFormat().format(date);
    }

    public static Date parse(String strDate) throws ParseException {
        return getDateFormat().parse(strDate);
    }

    public static String format_ts(Long date) throws ParseException {
        return getDateFormat().format(date);
    }

}
