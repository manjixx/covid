package cn.covid.bean;

import lombok.Data;


/**
 * Author：
 * Date：2022/5/3116:33
 * Desc: 用来封装各省市疫情数据的JavaBean
 */
@Data
public class CovidBean {
    private String provinceName;            // 省份
    private String provinceShortName;       // 省份名缩写
    private String cityName;                // 城市名称
    private Integer currentConfirmedCount;  // 当前确诊人数
    private Integer confirmedCount;         // 累计确诊人数
    private Integer suspectedCount;         // 疑似病例数
    private Integer curedCount;             // 治愈人数
    private Integer deadCount;              // 死亡人数
    private Integer locationId;             // 位置id
    private Integer pid;                    // 位置id
    private String statisticsData;          // 每一天的统计数据
    private String cities;                  // 下属城市
    private String datetime;                // 爬取数据时间

}
