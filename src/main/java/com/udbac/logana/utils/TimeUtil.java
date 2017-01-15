package com.udbac.logana.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by root on 2017/1/13.
 */
public class TimeUtil {

    private static final String DATE_FORMAT = "yyyy-MM-dd";

    private static final String TIME_FORMAT = "HH:mm:ss";

    private static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    /**
     * 处理时间 +8 小时
     * @param dateTime DATE+" "+TIME
     * @return DATE+" "+TIME
     */
    public static String handleTime(String dateTime) {
        String realtime = null;
        AtomicReference<Calendar> calendar;
        calendar = new AtomicReference<>(Calendar.getInstance());
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
        try {
            Date date = dateFormat.parse(dateTime);
            calendar.get().setTime(date);
            calendar.get().add(Calendar.HOUR_OF_DAY, 7);
            calendar.get().add(Calendar.MINUTE, 59);
            calendar.get().add(Calendar.SECOND, 59);
            realtime = (new SimpleDateFormat(DATE_TIME_FORMAT)).format(calendar.get().getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return realtime;
    }

}
