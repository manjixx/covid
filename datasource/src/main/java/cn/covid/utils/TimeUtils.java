package cn.covid.utils;

import org.apache.commons.lang3.time.FastDateFormat;

/**
 * Author：
 * Date：2022/5/3115:49
 * Desc: 时间类接口工具
 */
public abstract class TimeUtils {
    public static String format(Long timeStap, String pattern){
        return FastDateFormat.getInstance(pattern).format(timeStap);
    }

    public static void main(String[] args){
        String format = TimeUtils.format(System.currentTimeMillis(), "yyyy-MM-dd");
        System.out.println(format);
    }
}
