package cn.covid.bean

/**
 * Author：
 * Date：2022/6/1016:06
 * Desc:
 */
case class CovidBean(
    provinceName: String , // 省份
    provinceShortName: String , // 省份名缩写
    cityName: String , // 城市名称
    currentConfirmedCount: Int , // 当前确诊人数
    confirmedCount: Int , // 累计确诊人数
    suspectedCount: Int , // 疑似病例数
    curedCount: Int , // 治愈人数
    deadCount: Int , // 死亡人数
    locationId: Int , // 位置id
    pid: Int ,
    statisticsData: String , // 每一天的统计数据
    cities: String , // 下属城市
    datetime: String // 爬取数据时间
  )
