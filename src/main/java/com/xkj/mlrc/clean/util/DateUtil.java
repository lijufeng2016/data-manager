package com.xkj.mlrc.clean.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author: lijf@2345.com
 * @Date: 2018/7/5 13:47
 * @Version: 1.0
 */
public class DateUtil {

    private DateUtil() { }

    public static final String DATE_FORMAT_MEDIUM = "yyyy-MM-dd";
    public static final String DATE_FORMAT_LONG= "yyyy-MM-dd HH:mm:ss";
    private static Logger logger = LoggerFactory.getLogger(DateUtil.class);

    /**
     * 判断时间是否在时间段内
     *
     * @param nowTime
     * @param beginTime
     * @param endTime
     * @return
     */
    public static boolean belongCalendar(Date nowTime, Date beginTime, Date endTime) {
        Calendar date = Calendar.getInstance();
        date.setTime(nowTime);

        Calendar begin = Calendar.getInstance();
        begin.setTime(beginTime);

        Calendar end = Calendar.getInstance();
        end.setTime(endTime);

        return (date.after(begin) && date.before(end));
    }
    /**
     * 格式化日期
     * @param fmt 格式
     * @return
     */
    public static String getCurrentFormatDate(String fmt) {
        String formatDate = "";
        try {
            SimpleDateFormat format = new SimpleDateFormat(fmt);
            Date date = new Date();
            formatDate = format.format(date);
        }catch (Exception e){
            logger.error(e.toString(),e);
        }
        return formatDate;
    }

    /**
     * 获取N天前后的凌晨零点  yyyy-MM-dd HH:mm:ss
     * @param n
     * @return
     */
    public static String getNDayBeforeOrAfterZeroMorning(int n) {
        Calendar instance = Calendar.getInstance();
        SimpleDateFormat sdfCn = new SimpleDateFormat(DATE_FORMAT_MEDIUM);
        instance.add(Calendar.DAY_OF_MONTH, n);
        Date parse = instance.getTime();
        String nDayBefore = sdfCn.format(parse);

        return nDayBefore+" 00:00:00";
    }
    /**
     *  获取前一天时间字符串，返回例子：2018-07-30
     */
    public static final String getYesterDay(){
        Date date=new Date();
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.add(Calendar.DATE,-1);
        date=calendar.getTime();
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT_MEDIUM);
        return formatter.format(date);
    }

    /**
     * 转换成Timestamp
     * @param formatDt
     * @return
     */
    public static long parseToTimestamp(String format,String formatDt) {
        SimpleDateFormat formatDate = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = formatDate.parse(formatDt);
            return date.getTime() / 1000;
        } catch (ParseException e) {
            logger.error(e.toString(),e);
        }
        return 0;
    }

    /**
     * 获取n天前的日期
     * @param format 格式
     * @param n  天数
     * @return
     * @throws ParseException
     */
    public static String getNDayFmtDAte(String format,int n)   {
        Calendar instance = Calendar.getInstance();
        SimpleDateFormat sdfCn = new SimpleDateFormat(format);
        instance.add(Calendar.DAY_OF_MONTH, n);
        Date parse = instance.getTime();
        return sdfCn.format(parse);
    }

    /**
     * 根据day获取n天前的日期
     * @param format 格式
     * @param n  天数
     * @return
     * @throws ParseException
     */
    public static String getNDayFmtByDay(String format,String day,int n)   {
        Calendar instance = Calendar.getInstance();
        SimpleDateFormat sdfCn = new SimpleDateFormat(format);
        try {
            Date date = sdfCn.parse(day);
            instance.setTime(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        instance.add(Calendar.DAY_OF_MONTH, n);
        Date parse = instance.getTime();
        return sdfCn.format(parse);
    }

    /**
     *   获取前天的日期
     */
    public static final String getBeforeYe(){
        Date date=new Date();
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.add(Calendar.DATE,-2);
        date=calendar.getTime();
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT_MEDIUM);
        return formatter.format(date);
    }

    /**
     *     获取前N天的日期(不包含今天)
     */
    public static List<String> getNday(Integer num){
        if(num==null||num<=0){
            return new ArrayList<>();
        }
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT_MEDIUM);
        List<String> li = new ArrayList<>();
        Date date=new Date();
        Calendar calendar = new GregorianCalendar();
        for(int i=num;i>=1;i--){
            calendar.setTime(date);
            calendar.add(Calendar.DATE,-i);
            li.add(formatter.format(calendar.getTime()));
        }
        return li;
    }

    /**
     * 格式化日期转为Date
     * @param fmtDate yyyy-MM-dd HH:mm:ss
     * @return date
     */
    public static Date parse2Date(String fmtDate){
        Date date;
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_LONG);
        try {
             date = sdf.parse(fmtDate);
        } catch (ParseException e) {
            date = new Date();
        }
        return date;
    }

    /**
     * 获取当天还剩余的时间，单位：S
     * @return ...
     */
    public static int getLeftSecondsToday(){
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_LONG);

        String nowStr = dateFormat.format(new Date());

        String patten = " ";
        String endStr = nowStr.substring(0,nowStr.indexOf(patten)) + " 23:59:59";
        int leftSeconds = 0;
        try {
             leftSeconds = Integer.valueOf((dateFormat.parse(endStr).getTime() - dateFormat.parse(nowStr).getTime()) / 1000+"");
        } catch (ParseException e) {
            logger.error(e.toString(),e);
        }
        return leftSeconds;
    }

    /**
     * 时间戳转换为格式化时间戳
     * @param timestamp
     * @return
     */
    public static String parseToFmtDateStr(Integer timestamp){
        SimpleDateFormat fmt = new SimpleDateFormat(DATE_FORMAT_LONG);
        return fmt.format(new Date(timestamp * 1000L));
    }

    /**
     * 根据两个日期获取两日期之间的日期
     * @param beginDay 格式为2019-05-08
     * @param endDay 格式为2019-05-09
     * @return
     */
    public static List<String> getDays(String beginDay,String endDay){
        // 返回的日期集合
        List<String> days = new ArrayList<>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date start = dateFormat.parse(beginDay);
            Date end = dateFormat.parse(endDay);

            Calendar tempStart = Calendar.getInstance();
            tempStart.setTime(start);

            Calendar tempEnd = Calendar.getInstance();
            tempEnd.setTime(end);
            tempEnd.add(Calendar.DATE, +1);
            while (tempStart.before(tempEnd)) {
                days.add(dateFormat.format(tempStart.getTime()));
                tempStart.add(Calendar.DAY_OF_YEAR, 1);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return days;
    }

    /**
     * 时间戳转换为格式化时间戳
     * @param timestamp
     * @return
     */
    public static String parseTimestamp2FmtDateStr(String format,Long timestamp){
        SimpleDateFormat fmt = new SimpleDateFormat(format);
        return fmt.format(new Date(timestamp * 1000L));
    }

    /**
     * 判断时间是否在时间段内
     * 传入24小时制格式，如01 03 表示凌晨1点与3点，15表示下午3点。
     * 04 到 19 表示凌晨4点到今天的下午7点
     * 19 到 09 表示晚上7点到第二天上午9点
     *
     * @param now ...
     * @param beginHour ...
     * @param endHour ...
     * @return ...
     */
    public static boolean judgeTimeBetween(Date now, String beginHour, String endHour) {
        int iBeginHour = Integer.valueOf(beginHour);
        int iEndHour = Integer.valueOf(endHour);
        if (iBeginHour == iEndHour) {
            return true;
        }
        Calendar date = Calendar.getInstance();
        date.set(Calendar.HOUR_OF_DAY, iBeginHour);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        Date beginTime = date.getTime();
        if (iEndHour == 0) {
            date.set(Calendar.HOUR_OF_DAY, 23);
            date.set(Calendar.MINUTE, 59);
            date.set(Calendar.SECOND, 59);
            Date endTime = date.getTime();
            if (now.after(beginTime) && now.before(endTime)) {
                return true;
            }
        }
        if (iBeginHour < iEndHour) {
            date.set(Calendar.HOUR_OF_DAY, iEndHour);
            date.set(Calendar.MINUTE, 0);
            date.set(Calendar.SECOND, 0);
            Date endTime = date.getTime();
            if (now.after(beginTime) && now.before(endTime)) {
                return true;
            }
        } else {
            date.setTime(now);
            int nowHour = date.get(Calendar.HOUR_OF_DAY);
            if (nowHour >= iBeginHour) {
                return true;
            } else {
                if (nowHour < iEndHour) {
                    return true;
                }
            }
        }
        return false;
    }

    public static void main(String[] args) {
        String nDayFmtDAte = DateUtil.getNDayFmtDAte("yyyy-MM-dd HH:mm:ss", 3);
        System.out.println(nDayFmtDAte);
    }
}
