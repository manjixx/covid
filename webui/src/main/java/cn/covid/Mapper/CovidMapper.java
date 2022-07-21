package cn.covid.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
* Author：
* Date：2022/7/2122:12
* Desc:
*/
@Mapper
public interface CovidMapper {
    @Select("select `name`, `buy` as `采购`, `assign` as `下拨`, `donate` as `捐赠`, `consume` as `消耗`, `needs` as `需求`, `storage` as `库存` from `covid19_material`;")
    List<Map<String,Object>> getCovidWz();

    @Select("select `datetime`, `currentConfirmedCount`, `confirmedCount`, `suspectedCount`, `curedCount`, `deadCount` from `summary_info` where `datetime`= #{datetime};")
    List<Map<String, Object>> getNationalData(String datetime);

    @Select("select `provinceShortName` as `name`, `confirmedCount` as `value` from `province_confirmed_count` where `datetime`= #{datetime};")
    List<Map<String, Object>> getNationalMapData(String datetime);

    @Select("select `dateId`, `confirmedIncr` as `新增确诊`, `confirmedCount` as `累计确诊`, `suspectedCount` as `疑似病例`, `curedCount` as `累计治愈`, `deadCount` as `累计死亡` from `nation_confirmedIncr`;")
    List<Map<String, Object>> getCovidTimeData();

    @Select("select `provinceShortName` as `name`, `confirmedCount` as `value` from `abroad_import` where `datetime`= #{datetime} order by `value` desc limit 10;")
    List<Map<String, Object>> getCovidImportData(String datetime);


}
